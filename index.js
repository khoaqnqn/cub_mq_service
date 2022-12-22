const amqplib = require( 'amqplib' );
const CryptoJS = require( 'crypto-js' );
const { config } = require( 'dotenv' );
const _ = require( 'lodash' );

const nodeEnv = process.env.NODE_ENV || 'development';

config({ path: `.env.${ nodeEnv }.local` });

/* MQ config */
const { MQ_PROTOCOL, MQ_HOST, MQ_VIRTUAL_PATH, MQ_USER, MQ_PASSWORD } = process.env;
/* End MQ config */

class MQSupport {

	static sequelizeConnection;

	static connection;
	static channels;
	static isAlive;

	static MQ_TOPICS = {};
	static encodingKeys;

	static init(topics, sequelizeConnection) {
		MQSupport.sequelizeConnection = sequelizeConnection;
		MQSupport.MQ_TOPICS = _.reduce( topics, ( memo, topic ) => {
			memo[ topic ] = `${ nodeEnv }.${ topic }`;

			return memo;
		}, MQSupport.MQ_TOPICS );
		MQSupport.encodingKeys = _.reduce( topics, ( memo, topic ) => {
			memo[ topic ] = CryptoJS.SHA256(topic).toString().slice(-10);

			return memo;
		}, {} );
	}

	static sendFunction(connectedChannel, topic) {
		return async msg => {
			const encodedMsg = CryptoJS.AES.encrypt(JSON.stringify(msg), MQSupport.encodingKeys[ topic ]).toString();

			await connectedChannel.channel.sendToQueue(MQSupport.MQ_TOPICS[ topic ], Buffer.from(encodedMsg), { persistent: true });
		};
	}

	static registerNewHookFunction(connectedChannel, topic) {
		return async callback => {
			if (_.isArray(callback)) {
				connectedChannel.hooks = callback;

				await connectedChannel.channel.consume(MQSupport.MQ_TOPICS[ topic ], MQSupport.consumeFunction(connectedChannel, topic));

				return;
			}

			if (connectedChannel.hooks.length) {
				if (!_.find(connectedChannel.hooks, callback)) connectedChannel.hooks.push(callback);

				console.log( `Topic "${ topic }" had been hooked with ${ callback.toString() }\n\n` );

				return;
			}

			connectedChannel.hooks = [ callback ];

			await connectedChannel.channel.consume(MQSupport.MQ_TOPICS[ topic ], MQSupport.consumeFunction(connectedChannel, topic));
		};
	}

	static consumeFunction(connectedChannel, topic) {
		console.log( `Topic "${ topic }" had been hooked with:\n${ _.map( connectedChannel.hooks, fn => fn.toString() ).join( '\n' ) }\n\n` );

		return async msg => {
			let transaction;

			try {
				let decodedMsg;

				try {
					const rawMsg = msg.content.toString();

					decodedMsg = JSON.parse(CryptoJS.AES.decrypt(rawMsg, MQSupport.encodingKeys[ topic ]).toString(CryptoJS.enc.Utf8));
				} catch (error) {
					decodedMsg = { error };
				}

				transaction = MQSupport.sequelizeConnection && await MQSupport.sequelizeConnection.transaction();

				for (let index = 0; index < connectedChannel.hooks.length; index++) {
					await connectedChannel.hooks[ index ](decodedMsg, transaction);
				}

				await connectedChannel.channel.ack(msg);

				transaction && await transaction.commit();
			} catch (error) {
				await connectedChannel.channel.nack(msg);

				transaction && await transaction.rollback();
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
				MQSupport.connection = await amqplib.connect(`${MQ_PROTOCOL}://${MQ_USER}:${MQ_PASSWORD}@${MQ_HOST}/${MQ_VIRTUAL_PATH}`);

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

				if (mode === 'sending') {
					if ( options && options.prefetch ) {
						MQSupport.channels[ channelKey ].channel.prefetch( 1 );
					}

					MQSupport.channels[ channelKey ].send = MQSupport.sendFunction(MQSupport.channels[ channelKey ], topic);
				} else if (mode === 'receiving') {
					MQSupport.channels[ channelKey ].registerNewHook = MQSupport
					.registerNewHookFunction(MQSupport.channels[ channelKey ], topic);
				}
			}

			await MQSupport.channels[ channelKey ].channel.assertQueue(encodedTopic, { durable: true });

			return MQSupport.channels[ channelKey ];
		} catch (error) {
			await MQSupport.retryChannel(mode, topic);
		}
	}

}

class MQService {

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
			const listenerConfigs = _.values(listeners);

			if (!listenerConfigs.length) return;

			if (_.some(listenerConfigs, config => config.length !== 3)) throw new Error('Invalid listener');

			const sortedListeners = _.sortBy( listenerConfigs, config => config[ 1 ] );

			for (let index = 0; index < sortedListeners.length; index++) {
				await MQService.recvMQMess(sortedListeners[index][0], sortedListeners[index][2]);
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

	static async recvMQMess(topic, callback) {
		try {
			if (!MQSupport.MQ_TOPICS[ topic ] || typeof callback !== 'function') throw new Error('Invalid input');

			const cachedChannel = await MQSupport.getConnectedChannel('receiving', topic);

			await cachedChannel.registerNewHook(callback);
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
