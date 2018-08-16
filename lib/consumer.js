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

  checkQueue(q, cb) {
    this.conn.createChannel(function (err, ch) {
      if (err) {
        logger.error("checkQueue: Failed to create channel", err)
        return cb(err);
      }
      return ch.checkQueue(q, cb);
    });
  }

  call(method, params, callback) {

    let self = this;

    // Make sure the method is registered

    // this.checkQueue(method, (err, ok) => {
    //   if(err) {
    //     return logger.error(`The requested method '${method}' is not registered`)
    //   }
    // })


    self.ch.assertQueue('', {exclusive: true}, function (err, q) {
      const correlationId = uuidv1();

      logger.debug(` [x] Requesting method '%s' =>`, method,params);

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

      self.ch.sendToQueue(method,
        Buffer.from(msg),
        {correlationId: correlationId, replyTo: q.queue});
    });
  }
}

module.exports = Consumer;