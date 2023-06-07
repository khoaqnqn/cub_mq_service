const amqplib = require( 'amqplib' );
const CryptoJS = require( 'crypto-js' );
const _ = require( 'lodash' );

let countRetryQueue = 0;

class MQSupport {

	static NODE_ENV;

	static sequelizeConnection;

	static connection;
	static channels;
	static isAlive;

	static MQ_CONFIGS = {};

	static MQ_TOPICS = {};
	static encodingKeys;

	static DLX_EXCHANGE = 'DLX';
	static TTL_EXCHANGE = 'TTL';

	static DELAY_QUEUE_TIME = 10000;
	static MAX_RETRY_QUEUE = 3;
	static MESSAGE_TTL_TIME = 30000;

	static config(configs) {
		MQSupport.MQ_CONFIGS = _.reduce( configs, ( memo, config, key ) => {
			memo[ key ] = config;

			return memo;
		}, MQSupport.MQ_CONFIGS );
	}

	static init(topics, sequelizeConnection) {
		MQSupport.sequelizeConnection = sequelizeConnection;

		MQSupport.NODE_ENV = MQSupport.MQ_CONFIGS.NODE_ENV || 'development';

		MQSupport.MQ_TOPICS = _.reduce( topics, ( memo, topic ) => {
			memo[ topic ] = `${MQSupport.NODE_ENV}.${ topic }`;

			return memo;
		}, MQSupport.MQ_TOPICS );
		MQSupport.encodingKeys = _.reduce( topics, ( memo, topic ) => {
			memo[ topic ] = CryptoJS.SHA256(topic).toString().slice(-10);

			return memo;
		}, {} );
	}

	static sendFunction = (connectedChannel, topic) => async msg => {
		const encodedMsg = MQSupport.MQ_CONFIGS.MQ_ENCRYPT_MESSAGES
			? CryptoJS.AES.encrypt(JSON.stringify(msg), MQSupport.encodingKeys[ topic ]).toString()
			: JSON.stringify(msg);

		await connectedChannel.channel.assertQueue(MQSupport.MQ_TOPICS[ topic ]);

		await connectedChannel.channel.bindQueue(MQSupport.MQ_TOPICS[ topic ], `${MQSupport.NODE_ENV}.${MQSupport.DLX_EXCHANGE}.${topic}`, '');

		await connectedChannel.channel.publish(`${MQSupport.NODE_ENV}.${MQSupport.DLX_EXCHANGE}.${topic}`, '', Buffer.from(encodedMsg), { persistent: true });
	};

	static registerNewHookFunction = (connectedChannel, topic) => async callback => {
		if (_.isArray(callback)) {
			connectedChannel.hooks = callback;

			await connectedChannel.channel.consume(MQSupport.MQ_TOPICS[ topic ], MQSupport.consumeFunction(connectedChannel, topic));

			return;
		}

		if (connectedChannel.hooks.length) {
			if (!_.find(connectedChannel.hooks, callback)) {
				connectedChannel.hooks.push(callback);

				console.log( `Topic "${ topic }" had been hooked with '${ callback.name }'` );
			}

			return;
		}

		connectedChannel.hooks = [ callback ];

		await connectedChannel.channel.consume(MQSupport.MQ_TOPICS[ topic ], MQSupport.consumeFunction(connectedChannel, topic));
	};

	static consumeFunction = (connectedChannel, topic) => {
		console.log( `Topic "${ topic }" had been hooked with:\n${ _.map( connectedChannel.hooks, fn => `'${ fn.name }'` ).join( '\n' ) }` );

		return async msg => {
			let transaction;

			try {
				let decodedMsg;

				try {
					const rawMsg = msg.content.toString();

					decodedMsg = JSON.parse(
						MQSupport.MQ_CONFIGS.MQ_ENCRYPT_MESSAGES
							? CryptoJS.AES.decrypt(rawMsg, MQSupport.encodingKeys[ topic ]).toString(CryptoJS.enc.Utf8)
							: rawMsg
					);
				} catch (error) {
					decodedMsg = { error };
				}

				transaction = MQSupport.sequelizeConnection && await MQSupport.sequelizeConnection.transaction();

				for (let index = 0; index < connectedChannel.hooks.length; index++) {
					await connectedChannel.hooks[ index ](decodedMsg, transaction);
				}

				(MQSupport?.MQ_CONFIGS?.MQ_AUTO_ACK === false)
					? await MQSupport.nack(connectedChannel, msg, transaction)
					: await MQSupport.ack(connectedChannel, msg, transaction);

				countRetryQueue = 0;	
			} catch (error) {
				transaction && await transaction.rollback();

				countRetryQueue++;

				if ( countRetryQueue <= MQSupport.MAX_RETRY_QUEUE ) {
					console.log(`${topic} retry ${countRetryQueue}`);

					await MQSupport.pushToDelayedQueueRetry(connectedChannel, topic, msg, countRetryQueue);
					await connectedChannel.channel.ack(msg);
				} else {
					await connectedChannel.channel.nack(msg);
					await connectedChannel.channel.close();

					countRetryQueue = 0;

					console.error( topic, error );
				}
			}
		};
	}

	static async retryConnection() {
		MQSupport.isAlive = undefined;

		await MQSupport.initConnection();

		if (!_.keys(MQSupport.channels).length) return;

		// recover all channels and hooks connected to each receiving channel
		await Promise.all(
			_.map(MQSupport.channels, async (oldChannel, key) => {
				const { mode, topic, hooks } = oldChannel;

				await MQSupport.getConnectedChannel(mode, topic);

				if (mode === 'receiving') MQSupport.channels[ key ].registerNewHook(hooks);
			})
		);
	}

	static async initConnection() {
		try {
			if (!MQSupport.isAlive) {
				MQSupport.connection = await amqplib
				.connect({
					protocol: MQSupport.MQ_CONFIGS.MQ_PROTOCOL,
					hostname: MQSupport.MQ_CONFIGS.MQ_HOST,
					port: MQSupport.MQ_CONFIGS.MQ_PORT,
					username: MQSupport.MQ_CONFIGS.MQ_USER,
					password: MQSupport.MQ_CONFIGS.MQ_PASSWORD,
					vhost: MQSupport.MQ_CONFIGS.MQ_VIRTUAL_PATH,
				});

				MQSupport.connection.on('error', async () => {
					await MQSupport.retryConnection();
				});

				MQSupport.connection.on('close', async () => {
					await MQSupport.retryConnection();
				});

				MQSupport.isAlive = true;
			}
		} catch (error) {
			await MQSupport.retryConnection();
		}
	}

	static async retryChannel(mode, topic) {
		const encodedTopic = MQSupport.MQ_TOPICS[ topic ];
		const channelKey = `${mode}_${encodedTopic}`;

		MQSupport.channels[ channelKey ].isAlive = undefined;

		const hooks = MQSupport.channels[ channelKey ].hooks;

		await MQSupport.getConnectedChannel(mode, topic);

		if (mode === 'receiving') MQSupport.channels[ channelKey ].registerNewHook(hooks);
	}

	static async closeChannel(mode, topic) {
		const encodedTopic = MQSupport.MQ_TOPICS[ topic ];
		const channelKey = `${mode}_${encodedTopic}`;

		if (!MQSupport.channels[ channelKey ]) throw new Error('Invalid input');

		await MQSupport.channels[ channelKey ].channel.close();

		delete MQSupport.channels[ channelKey ];
	}

	static async getConnectedChannel(mode, topic, options) {
		const encodedTopic = MQSupport.MQ_TOPICS[ topic ];
		const channelKey = `${mode}_${encodedTopic}`;

		await MQSupport.initConnection();

		if (!MQSupport.channels) MQSupport.channels = {};

		if (!MQSupport.channels[ channelKey ]) MQSupport.channels[ channelKey ] = { mode, topic, hooks: [] };

		try {
			if (!MQSupport.channels[ channelKey ].isAlive) {
				MQSupport.channels[ channelKey ].channel = await MQSupport.connection.createChannel();

				MQSupport.channels[ channelKey ].channel.on('error', async () => {
					await MQSupport.retryChannel(mode, topic);
				});

				MQSupport.channels[ channelKey ].mode = mode;
				MQSupport.channels[ channelKey ].topic = topic;
				MQSupport.channels[ channelKey ].isAlive = true;

				if ( !options || !_.has( options, 'prefetch' ) || options.prefetch === true ) {
					MQSupport.channels[ channelKey ].channel.prefetch( 1 );
				}

				if (mode === 'sending') {
					MQSupport.channels[ channelKey ].send = MQSupport.sendFunction(MQSupport.channels[ channelKey ], topic);
				} else if (mode === 'receiving') {
					MQSupport.channels[ channelKey ].registerNewHook = MQSupport.registerNewHookFunction(MQSupport.channels[ channelKey ], topic);
				}
			}
		
			const TTL_PREFIX_EXCHANGE = `${MQSupport.NODE_ENV}.${MQSupport.TTL_EXCHANGE}.${topic}`;
			const DLX_PREFIX_EXCHANGE = `${MQSupport.NODE_ENV}.${MQSupport.DLX_EXCHANGE}.${topic}`;

			await MQSupport.channels[ channelKey ].channel.assertExchange(TTL_PREFIX_EXCHANGE, 'direct', { durable: true } );
			await MQSupport.channels[ channelKey ].channel.assertExchange(DLX_PREFIX_EXCHANGE, 'direct', { durable: true } );

			await MQSupport.channels[ channelKey ].channel.assertQueue(encodedTopic, { durable: true });
			await MQSupport.channels[ channelKey ].channel.assertQueue(`${encodedTopic}-RETRY`, { durable: true, deadLetterExchange: DLX_PREFIX_EXCHANGE, messageTtl: MQSupport.MESSAGE_TTL_TIME });

			await MQSupport.channels[ channelKey ].channel.bindQueue(encodedTopic, DLX_PREFIX_EXCHANGE);
			await MQSupport.channels[ channelKey ].channel.bindQueue(`${encodedTopic}-RETRY`, TTL_PREFIX_EXCHANGE);

			return MQSupport.channels[ channelKey ];
		} catch (error) {
			await MQSupport.retryChannel(mode, topic);
		}
	}

	static async recvMQMess(topic, callback) {
		try {
			if (
				!MQSupport.MQ_TOPICS[ topic ]
				|| ( !_.isFunction( callback ) && !_.isArray( callback ) )
			) throw new Error('Invalid input');

			const cachedChannel = await MQSupport.getConnectedChannel('receiving', topic);

			await cachedChannel.registerNewHook(callback);
		} catch (error) {
			throw error;
		}
	}

	static async ack(connectedChannel, msg, transaction = undefined) {
		await connectedChannel.channel.ack(msg);

		transaction && await transaction.commit();
	}

	static async nack(connectedChannel, msg, transaction = undefined) {
		await connectedChannel.channel.nack(msg);

		transaction && await transaction.rollback();
	}

	static async pushToDelayedQueueRetry(connectedChannel, topic, msg, countRetryQueue) {
		await connectedChannel.channel.publish(`${MQSupport.NODE_ENV}.${MQSupport.TTL_EXCHANGE}.${topic}`, '', Buffer.from(msg.content.toString()), {
			persistent: true,
			headers: {
				'x-delay': MQSupport.DELAY_QUEUE_TIME,
				'x-retry-count': countRetryQueue
			}
		} );
	}

}

class MQService {

	static config(configs) {
		try {
			MQSupport.config(configs);
		} catch (error) {
			throw error;
		}
	}

	static init(topics, sequelizeConnection) {
		try {
			if (!_.keys(topics).length || _.keys(MQSupport.MQ_TOPICS).length) throw new Error('Invalid input');

			MQSupport.init(topics, sequelizeConnection);
		} catch (error) {
			throw error;
		}
	}

	static async hookListeners(listeners) {
		try {
			const groupByTopicObj = _.chain(listeners)
			.reduce((memo, listener) => {
				if ( !memo[ listener[0] ] ) memo[ listener[0] ] = [];

				memo[ listener[0] ].push(listener);

				return memo;
			}, {})
			.values()
			.value();

			for (let index = 0; index < groupByTopicObj.length; index++) {
				await MQSupport.recvMQMess(groupByTopicObj[index][0][0], _.chain(groupByTopicObj[index]).sortBy(i=>i[1]).map(i=>i[2]).value());
			}
		} catch (error) {
			throw error;
		}
	}

	static async sendMQMess(topic, msg, options = {}) {
		try {
			if (!MQSupport.MQ_TOPICS[ topic ] || !_.isObject(msg)) throw new Error('Invalid input');

			const cachedChannel = await MQSupport.getConnectedChannel('sending', topic, options);

			await cachedChannel.send(msg);
		} catch (error) {
			throw error;
		}
	}

	static async closeChannel(topic) {
		try {
			if (!MQSupport.MQ_TOPICS[ topic ]) throw new Error('Invalid input');

			await MQSupport.closeChannel('receiving', topic);
		} catch (error) {
			throw error;
		}
	}

}

module.exports = {
	MQService,
	MQ_TOPICS: MQSupport.MQ_TOPICS,
};
