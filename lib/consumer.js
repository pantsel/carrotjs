const amqp = require('amqplib/callback_api');
const logger = require('./logger');
const _ = require('lodash');
const uuidv1 = require('uuid/v1');
const Response = require('./response');

class Consumer {
  constructor() {

  }

  async connect(uri) {
    let self = this;
    this.uri = uri;
    return new Promise((resolve, reject) => {
      amqp.connect(this.uri, function (err, conn) {
        if (err) {
          logger.error("Failed to connect to amqp broker", err)
          process.exit(1);
        }

        logger.info("Connected to amqp broker")
        self.conn = conn;

        conn.createChannel(function (err, ch) {
          if (err) {
            logger.error("Failed to create channel", err)
            process.exit(1);
          }

          logger.info("Created channel => %d", ch.ch)
          self.ch = ch;

          resolve(self);
        });
      });
    })
  }

  call(uri, params, callback) {

    let self = this;
    this.ch.assertQueue('', {exclusive: true}, function (err, q) {
      const correlationId = uuidv1();

      logger.debug(` [x] Requesting method '%s' =>`, uri,params);

      self.ch.consume(q.queue, function (msg) {
        if (msg.properties.correlationId == correlationId) {
          callback(Response.json(msg));
        }
      }, {noAck: true});

      let msg;
      try {
        msg = _.isString(params) ? params : JSON.stringify(params);
      }catch(err) {
        msg = params.toString();
      }

      self.ch.sendToQueue(uri,
        Buffer.from(msg),
        {correlationId: correlationId, replyTo: q.queue});
    });
  }
}

module.exports = Consumer;