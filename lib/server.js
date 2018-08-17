"use strict";

const amqp = require('amqplib/callback_api');
const logger = require('./logger');
const _ = require('lodash');
const Response = require('./response');
const path = require('path');
const includeAll = require('include-all');
const uuidv1 = require('uuid/v1');
const Utils = require('./utils');

class Server {

  constructor() {}

  /**
   * Initializes Config properties
   * @param config
   * @returns {Server}
   */
  config(config) {
    this.config = config || {};
    // Logger config
    logger.level = _.get(this.config,'logger.level', logger.level);

    return this;
  }

  /**
   * Connects to the message Broker
   * @param uri
   * @returns {Promise<any>}
   */
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

  /**
   * Registers a procedure
   * @param q
   * @param options
   * @param callback
   */
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

  /**
   * Calls a remote procedure
   * @param method
   * @param params
   * @param callback
   */
  call(method, params, callback) {

    let self = this;

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

  /**
   * Sends message to queue
   * @param q
   * @param options
   * @param msg
   */
  send(q, options, msg) {
    msg = msg || options;
    this.ch.assertQueue(q, _.merge({durable: false},options));
    this.ch.sendToQueue(q, Buffer.from(Utils.stringOf(msg)));
    logger.debug("Send:Sent %s", Utils.stringOf(msg));
  }


  declareExchange(ex, type = 'direct', q = '', options, cb) {
    let self = this;
    this.ch.assertExchange(ex, type, _.merge({durable: true},options));
    this.ch.assertQueue(q, {exclusive: true}, function(err, _q) {
      logger.debug(" [declareExchange]: Waiting for messages in %s.", _q.queue);
      self.ch.bindQueue(_q.queue, ex, '');

      self.ch.consume(_q.queue, function(msg) {
        logger.debug("[x] %s", msg.content.toString());
        self.ch.ack(msg);
        cb(Response.json(msg));
      }, {noAck: false});
    });
  }


  /**
   * Publishes to an exchange.
   * ------------------------
   * In order not to mess with the rpc channel's stability,
   * this method creates a new channel and closes it after publishing is done.
   * @param ex
   * @param routingKey
   * @param msg
   * @param options
   */
  publish(exchange, routingKey, msg, options, exchangeType = 'direct') {

    this.conn.createChannel(function (err, ch) {
      if (err) {
        return logger.error("[publish]: Failed to create channel", err)
      }
      ch.assertExchange(exchange, exchangeType,  _.merge({durable: false},options));
      ch.publish(exchange, routingKey || '', new Buffer(Utils.stringOf(msg)));
      logger.debug("[publish]: Sent %s", Utils.stringOf(msg));
      ch.close(err => {
        if(err) {
          return logger.error("[publish]: Failed to close channel");
        }
        logger.debug("[publish]: Channel closed");
      })
    });

  }

  subscribe(q, options, cb) {
    this.ch.assertQueue(q,  _.merge({durable: false},options));
    logger.debug("[Subscribe:] Waiting for messages in %s. To exit press CTRL+C", q);
    this.ch.consume(q, function(msg) {
      logger.debug("[Subscribe:] Received %s", msg.content.toString());
      cb(Response.json(msg));
    }, {noAck: true});
  }

  /**
   * Includes custom configuration from a config folder
   */
  includeConfig() {

    let includes = includeAll({
      dirname     :  path.join(process.cwd(),_.get(this.config,'configDir') || 'config'),
      filter      :  /(.+)\.js$/,
      optional    :  true
    });

    this.config = _.merge(this.config || {}, includes)

    // Initialize procedures
    for(let key in this.config.procedures) {
      if(!this.config.procedures[key].handler) {
        logger.warn(`No handler defined for procedure "${key}"`)
      }else{
        this.register(key, this.config.procedures[key].options || {}, this.config.procedures[key].handler);
      }
    }

    // Initialize exchanges
    for(let key in this.config.exchanges) {
      if(!this.config.exchanges[key].handler) {
        logger.warn(`No handler defined for exchange "${key}"`)
      }else{
        this.declareExchange(
          key,
          this.config.exchanges[key].type,
          this.config.exchanges[key].queue,
          this.config.exchanges[key].options || {},
          this.config.exchanges[key].handler
        );
      }
    }

  }
}

module.exports = new Server();