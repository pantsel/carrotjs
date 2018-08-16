"use strict";

const amqp = require('amqplib/callback_api');
const logger = require('./logger');
const _ = require('lodash');
const Response = require('./response');

let Server = module.exports = {

  connect: async (uri) => {
    return new Promise((resolve, reject) => {
      amqp.connect(uri, function (err, conn) {
        if (err) {
          logger.error("Failed to connect to amqp broker", err)
          process.exit(1);
        }

        logger.info("Connected to amqp broker")

        conn.createChannel(function (err, ch) {
          if (err) {
            logger.error("Failed to create channel", err)
            process.exit(1);
          }

          logger.info("Created channel => %d", ch.ch)

          resolve({
            ch: ch,
            register: Server.register
          });
        });
      });
    })
  },


  register(q, options, callback) {

    let self = this;

    if (!_.isString(q)) {
      logger.error("`q` parameter must be a String");
      return;
    }

    if (options && !_.isObject(options)) {
      logger.error("`options` parameter must be an Object");
      return;
    }

    let defaultOptions = {durable: false};

    this.ch.assertQueue(q, _.merge(defaultOptions,options));
    this.ch.prefetch(1);
    console.log(` [x] Awaiting RPC requests @${q}`);
    // this.ch.consume(q, callback)
    this.ch.consume(q, function reply(msg) {

      // var n = parseInt(msg.content.toString());
      //
      // console.log(" [.] fib(%d)", n);
      //
      // let r = self.fibonacci(n);
      //

      let data;
      try {
        data  = JSON.parse(msg.content.toString());
      }catch (e) {
        data = msg.content.toString();
      }

      callback(data, function (response) {

        self.ch.sendToQueue(msg.properties.replyTo,
          Buffer.from(Response.string(response)),
          {correlationId: msg.properties.correlationId});

        self.ch.ack(msg);
      })


    })
  }
}
