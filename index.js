/* eslint-disable require-jsdoc */
const amqplib = require( 'amqplib' );
const CryptoJS = require( 'crypto-js' );
const _ = require( 'lodash' );
const { config } = require( 'dotenv' );

config({ path: `.env.${ process.env.NODE_ENV || 'development' }.local` });

/* MQ config */
const { MQ_PROTOCOL, MQ_HOST, MQ_VIRTUAL_PATH, MQ_USER, MQ_PASSWORD } = process.env;
/* End MQ config */

/* Force topics config */
const MQ_TOPICS = {
	REQUEST_APP_INIT: 'REQUEST_APP_INIT',
	APP_INIT: 'APP_INIT',
	CREATE_ACCOUNT: 'CREATE_ACCOUNT',
	CREATE_WORKSPACE: 'CREATE_WORKSPACE',
	UPDATE_WORKSPACE: 'UPDATE_WORKSPACE',
};
const hashedTopics = _.reduce(
	MQ_TOPICS,
	(memo, topic) => {
		memo[ topic ] = CryptoJS.SHA256(topic).toString();

		return memo;
	},
	{}
);
const topicNames = _.reduce(
	MQ_TOPICS,
	(memo, topic) => {
		// memo[ topic ] = hashedTopics[ topic ].slice(0, 10);
		memo[ topic ] = topic;

		return memo;
	},
	{}
);
const encodingKeys = _.reduce(
	MQ_TOPICS,
	(memo, topic) => {
		memo[ topic ] = hashedTopics[ topic ].slice(-10);

		return memo;
	},
	{}
);
/* End Force topics config */

class MQSupport {

	static connection;

	static channels;

	static isAlive;

	static sendFunction(connectedChannel, topic) {
		return msg => {
			const encodedMsg = CryptoJS.AES.encrypt(JSON.stringify(msg), encodingKeys[ topic ]).toString();

			connectedChannel.channel.sendToQueue(topicNames[ topic ], Buffer.from(encodedMsg), { persistent: true });
		};
	}

	static registerNewHookFunction(connectedChannel, topic) {
		return callback => {
			if (_.isArray(callback)) {
				connectedChannel.hooks = callback;

				connectedChannel.channel.consume(topicNames[ topic ], MQSupport.consumeFunction(connectedChannel, topic));

				return;
			}

			if (connectedChannel.hooks.length) {
				if (!_.find(connectedChannel.hooks, callback)) connectedChannel.hooks.push(callback);

				return;
			}

			connectedChannel.hooks = [ callback ];

			connectedChannel.channel.consume(topicNames[ topic ], MQSupport.consumeFunction(connectedChannel, topic));
		};
	}

	static consumeFunction(connectedChannel, topic) {
		return async msg => {
			let decodedMsg;

			try {
				const rawMsg = msg.content.toString();

				decodedMsg = JSON.parse(CryptoJS.AES.decrypt(rawMsg, encodingKeys[ topic ]).toString(CryptoJS.enc.Utf8));
			} catch (error) {
				decodedMsg = { error };
			}

			for (let index = 0; index < connectedChannel.hooks.length; index++) {
				if (index < connectedChannel.hooks.length - 1) {
					await connectedChannel.hooks[ index ](decodedMsg);

					continue;
				}

				await connectedChannel.hooks[ index ](decodedMsg, connectedChannel.channel.ack.bind(connectedChannel.channel, msg));
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
		const encodedTopic = topicNames[ topic ];
		const channelKey = `${mode}_${encodedTopic}`;

		MQSupport.channels[ channelKey ].isAlive = undefined;

		const hooks = MQSupport.channels[ channelKey ].hooks;

		await MQSupport.getConnectedChannel(mode, topic);

		if (mode === 'receiving') MQSupport.channels[ channelKey ].registerNewHook(hooks);
	}

	static async getConnectedChannel(mode, topic, options) {
		const encodedTopic = topicNames[ topic ];
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

				MQSupport.channels[ channelKey ].channel.on('close', async () => {
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

	static async sendMQMess(topic, msg, options = {}) {
		try {
			if (!MQ_TOPICS[ topic ] || !_.isObject(msg)) throw new Error('Invalid input');

			const cachedChannel = await MQSupport.getConnectedChannel('sending', topic, options);

			cachedChannel.send(msg);
		} catch (error) {
			throw error;
		}
	}

	static async recvMQMess(topic, callback) {
		try {
			if (!MQ_TOPICS[ topic ] || typeof callback !== 'function') throw new Error('Invalid input');

			const cachedChannel = await MQSupport.getConnectedChannel('receiving', topic);

			cachedChannel.registerNewHook(callback);
		} catch (error) {
			throw error;
		}
	}

}

module.exports = {
	MQService,
	MQ_TOPICS,
};
