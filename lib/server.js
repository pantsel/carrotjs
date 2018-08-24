"use strict";

const amqp = require('amqplib');
const Errors = require('./errors');
const _ = require('lodash');
const Response = require('./response');
const path = require('path');
const uuidv1 = require('uuid/v1');
const Utils = require('./utils');
const yaml = require('js-yaml');
const fs   = require('fs');

let logger;

class Server {

  constructor() {}

  /**
   * Initializes Config properties
   * @param config
   * @returns {Server}
   */
  config(config) {
    this.config = config || {};
    
    this.url = this.config.url;
    // logger config
    logger = require('./logger').init(_.get(this.config, 'logger', {}))

    return this;
  }


  /**
   * Connects to the message Broker
   * @param url
   * @returns {Promise<any>}
   */
  async connect() {

    let self = this;

    this.conn = await amqp.connect(this.url);
    logger.debug("Connected to broker")

    this.ch = await this.conn.createChannel();
    logger.debug("Channel created", this.ch)

    /**
     * Listen to channel events
     * http://www.squaremobius.net/amqp.node/channel_api.html#channel_events
     */
    this.ch.on('close', function () {
      logger.debug("Channel closed")
      // setTimeout(async  () => {
      //   await self.connect();
      // }, 2000)
    })

    this.ch.on('error', function (error) {
      logger.error("Channel error %j", error)
    })

    this.ch.on('return', function (msg) {
      logger.debug("Channel returned a message %json", msg)

      let fields = _.get(msg,'fields',{});
      let properties = _.get(msg,'properties',{});
      let replyTo = properties.replyTo;
      let correlationId = properties.correlationId;

      // Reply to queue that triggered this event
      if(replyTo) {
        let errorExtras = _.merge(fields,{properties: properties})
        let error = new Errors.AmqpError(fields.replyText, errorExtras)
        let options = _.merge({replyTo: replyTo}, (correlationId ? { correlationId:correlationId } : {}));
        self.ch.sendToQueue(replyTo,
          Buffer.from(Utils.stringOf(error)),
          options);
      }
    })

    this.ch.on('drain', function () {
      logger.debug("Channel's write buffer has been emptied")
    })

    await this.includeConfig();

    return this;
  }

  /**
   * Registers a procedure
   * @param q
   * @param options
   * @param callback
   */
  async register(q, options, callback) {

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

    await this.ch.assertQueue(q, {durable: false});
    this.ch.prefetch(1);
    logger.debug(`[x] Awaiting RPC requests @${q}`);

    await this.ch.consume(q, function reply(msg) {

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
  async call(method, params, callback) {

    let q = await this.ch.assertQueue('', {exclusive: true})
    const correlationId = uuidv1();

    let msg;
    try {
      msg = _.isString(params) ? params : JSON.stringify(params);
    } catch (err) {
      msg = params.toString();
    }

    logger.debug(`[x] Consumer: Calling '%s' with args:`, method, msg);

    this.ch.consume(q.queue, function (msg) {
      if (msg.properties.correlationId == correlationId) {
        let response = Response.json(msg);
        response.name === 'AmqpError' ? callback(response) : callback(null, response);
      }
    }, {noAck: true});

    this.ch.sendToQueue(method,
      Buffer.from(msg),
      {correlationId: correlationId, replyTo: q.queue, mandatory: true});
  }

  /**
   * Sends message to queue
   * @param q
   * @param options
   * @param msg
   */
  send(q, options, msg) {
    msg = msg || options;
    this.ch.assertQueue(q, _.merge({durable: false}, options));
    this.ch.sendToQueue(q, Buffer.from(Utils.stringOf(msg)));
    logger.debug("Send:Sent %s", Utils.stringOf(msg));
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
  async publish(exchange, routingKey, msg, options) {

    let ch = await this.conn.createChannel();
    ch.publish(exchange, routingKey || '', new Buffer(Utils.stringOf(msg)));
    logger.debug("[publish]: Sent %s", Utils.stringOf(msg));
    ch.close(err => {
      if (err) {
        logger.error("[publish]: Failed to close channel");
      }else{
        logger.debug("[publish]: Channel closed");
      }
    })

  }

  async subscribe(q, cb, options = {}) {
    options = _.merge({noAck: true}, options);

    logger.debug("[Subscribe:] Waiting for messages in %s", q);
    let self = this;
    await this.ch.consume(q, function (msg) {
      logger.debug("[Subscribe:] Received %s", msg.content.toString());
      cb(Response.json(msg));
      if(!options.noAck) self.ch.ack(msg);
    }, options);
  }


  /**
   * Parses json or yaml config file.
   * @returns {boolean}
   */
  parseConfigFile() {
    let configDir = path.join(process.cwd(), _.get(this.config, 'configDir') || 'config');
    let config;

    try {
      config = yaml.safeLoad(fs.readFileSync(path.join(configDir,'config.yml'), 'utf8'));
    } catch (e) {
      logger.warn("Invalid config file %j", e);
      return false;
    }

    this.config = _.merge(this.config || {}, config);
    return true;

  }


  /**
   * Sets up exchanges as defined in config.
   * @returns {Promise<boolean>}
   */
  async setupExchanges() {
    // Setup exchanges
    let exchanges = this.config.exchanges;
    if(!_.isObject(exchanges)) {
      logger.warn("Invalid exchanges configuration. Exchanges must be an Object");
      return false;
    }


    logger.debug("----------------------------------------------------------")
    logger.debug("Asserting exchanges");
    logger.debug("----------------------------------------------------------")

    for(let key in exchanges) {
      let exchange_name = key;
      let type = exchanges[key].type;
      let options = exchanges[key].options;
      let args = exchanges[key].args; // ToDo implement RabbitMQ's `alternateExchange`
      let bindings = exchanges[key].queues;

      await this.ch.assertExchange(exchange_name, type, options);
      logger.debug("Assert exchange: %s", exchange_name);

      if(bindings && _.isObject(bindings)) {
        // Create queue bindings
        for(let key in bindings) {
          let queue = {
            name: key,
            options: bindings[key].options,
            routingKey: bindings[key].routingKey,
            args: bindings[key].args,
          };

          let q = await this.ch.assertQueue(queue.name, queue.options);
          logger.debug("Assert queue: %s", q.queue);
          await this.ch.bindQueue(q.queue, exchange_name, queue.routingKey, queue.args)
          logger.debug("Bound queue `%s` to exchange `%s` using routingKey `%s`", q.queue, exchange_name, queue.routingKey || '');
        }
      }
    }
  }

  /**
   * Sets up orphaned queues as defined in config
   * @returns {Promise<void>}
   */
  async setupQueues() {
    // Setup orphaned queues
    let queues = this.config.queues;
    if(!_.isObject(queues)) {
      logger.warn("Invalid queues configuration. Queues must be an Object");
      return false;
    }

    logger.debug("----------------------------------------------------------")
    logger.debug("Asserting orphaned queues");
    logger.debug("----------------------------------------------------------")

    for(let key in queues) {
      let queue_name = key;
      let options = queues[key].options;

      let q = await this.ch.assertQueue(queue_name, options);
      logger.debug("Assert queue: %s", q.queue);
    }
  }

  /**
   * Registers subscriptions to asserted queues
   * @returns {Promise<void>}
   */
  async registerSubscriptions() {
    // Setup orphaned queues
    let subs = this.config.subscriptions;
    if(!_.isObject(subs)) {
      logger.warn("Invalid subscriptions configuration. Subscriptions must be an Object");
      return false;
    }

    logger.debug("----------------------------------------------------------")
    logger.debug("Registering subscriptions");
    logger.debug("----------------------------------------------------------")

    for(let key in subs) {
      let queue_name = key;
      let handler = subs[key].handler;
      let handlerPath = handler.substr(0,handler.indexOf('.'));
      let handlerMethods = handler.substr(handler.indexOf('.') +1 );
      let handlerFile = require(path.join(process.cwd(),handlerPath));
      let options = subs[key].options;

      await this.subscribe(queue_name, this.toMethod(handlerFile, handlerMethods), options);
      logger.debug("Subscribed to queue: %s", queue_name);
    }
  }


  /**
   * Sets up procedures for RPC as defined in config.
   * @returns {Promise<void>}
   */
  async registerProcedures() {
    let procedures = this.config.procedures;
    if(!_.isObject(procedures)) {
      logger.warn("Invalid procedures configuration. Procedures must be an Object");
      return false;
    }

    logger.debug("----------------------------------------------------------")
    logger.debug("Registering procedures");
    logger.debug("----------------------------------------------------------")

    for (let key in procedures) {
      let handler = procedures[key].handler;
      if (!handler) {
        logger.warn(`No handler defined for procedure "${key}"`)
      } else {

        let handlerPath = handler.substr(0,handler.indexOf('.'));
        let handlerMethods = handler.substr(handler.indexOf('.') +1 );
        let handlerFile = require(path.join(process.cwd(),handlerPath));

        await this.register(key, procedures[key].options || {}, this.toMethod(handlerFile, handlerMethods));
      }
    }
  }

  toMethod(result, str) {
    let methods = str.split(".");
    for(let i in methods) {
      result = result[methods[i]];
    }
    return result;
  }


  /**
   * Includes custom configuration from a config folder
   */
  async includeConfig() {

    if(this.parseConfigFile()) {
      logger.debug("Loaded config");
      logger.debug("%j", this.config);

      await this.setupExchanges();
      await this.setupQueues();
      await this.registerProcedures();
      await this.registerSubscriptions();

      logger.debug("----------------------------------------------------------")
      logger.debug("Configuration completed");
      logger.debug("----------------------------------------------------------")
    }
  }

  /**
   * Checks if queue exists.
   * Opens an ephemeral channel to do so.
   * @param q
   */
  async checkQueue(q, cb) {
    logger.debug("Checking if queue `%s` exists", q);
    this.conn.createChannel(function (err, ch) {
      if (err) {
        logger.error("Failed to create channel", err)
        return cb(err);
      }

      ch.checkQueue(q, cb);
    });
  }

}

module.exports = new Server();