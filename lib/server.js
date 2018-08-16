"use strict";

const amqp = require('amqplib/callback_api');
const logger = require('./logger');
const _ = require('lodash');
const Response = require('./response');
const path = require('path');

class Server {
  constructor(config) {
    this.config = config;
  }

  async connect() {
    let self = this;
    return new Promise((resolve, reject) => {
      amqp.connect(this.config.uri, function (err, conn) {
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

          self.autoRegisterProcedures();
          resolve(self);
        });
      });
    })
  }

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

    logger.debug("Registering procedure: %s", q);

    let defaultOptions = {durable: false};

    this.ch.assertQueue(q, _.merge(defaultOptions, options));
    this.ch.prefetch(1);
    console.log(` [x] Awaiting RPC requests @${q}`);

    this.ch.consume(q, function reply(msg) {

      let data;
      try {
        data = JSON.parse(msg.content.toString());
      } catch (e) {
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

  autoRegisterProcedures() {
    try{
      let proceduresConfig = require(path.join(process.cwd(),_.get(this.config,'proceduresPath') || 'config/procedures'));
      logger.debug("Server:autoRegisterProcedures:called");
      for(let key in proceduresConfig) {
        this.register(key, proceduresConfig[key].options || {}, proceduresConfig[key].handler);
      }
    }catch(err) {
      logger.error(`Failed to register procedures. Path '${this.config.proceduresPath}' does not exist.`)
    }

  }
}

module.exports = Server;