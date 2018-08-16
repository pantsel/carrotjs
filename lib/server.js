"use strict";

const amqp = require('amqplib/callback_api');
const logger = require('./logger');
const _ = require('lodash');
const Response = require('./response');
const path = require('path');
const includeAll = require('include-all');
const uuidv1 = require('uuid/v1');

class Server {

  constructor() {
  }

  config(config) {
    this.config = config || {};

    // Logger config
    logger.level = _.get(this.config,'logger.level', logger.level);

    return this;
  }

  connect(uri) {
    let self = this;
    this.uri = uri;
    return new Promise((resolve, reject) => {
      amqp.connect(uri, function (err, conn) {
        if (err) {
          logger.error("Failed to connect to amqp broker at %s", uri, err)
          return reject(err);
        }

        logger.info("Connected to amqp broker")
        self.conn = conn;

        conn.createChannel(function (err, ch) {
          if (err) {
            logger.error("Failed to create channel", err)
            return reject(err);
          }

          logger.info("Created channel")
          self.ch = ch;

          self.includeConfig();
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
    logger.debug(`[x] Awaiting RPC requests @${q}`);

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

      let msg;
      try {
        msg = _.isString(params) ? params : JSON.stringify(params);
      }catch(err) {
        msg = params.toString();
      }

      logger.debug(`[x] Consumer: Calling '%s' with args:`, method, msg);

      self.ch.consume(q.queue, function (msg) {
        if (msg.properties.correlationId == correlationId) {
          callback(Response.json(msg));
        }
      }, {noAck: true});


      self.ch.sendToQueue(method,
        Buffer.from(msg),
        {correlationId: correlationId, replyTo: q.queue});
    });
  }

  includeConfig() {

    let includes = includeAll({
      dirname     :  path.join(process.cwd(),_.get(this.config,'configDir') || 'config'),
      filter      :  /(.+)\.js$/,
      optional    :  true
    });

    this.config = _.merge(this.config || {}, includes)

    for(let key in this.config.procedures) {
      // A handler is required
      if(!this.config.procedures[key].handler) {
        logger.warn(`No handler defined for procedure "${key}"`)
      }else{
        this.register(key, this.config.procedures[key].options || {}, this.config.procedures[key].handler);
      }
    }

  }
}

module.exports = new Server();