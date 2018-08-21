"use strict";

const amqp = require('amqplib');
const _ = require('lodash');
const Response = require('./response');
const path = require('path');
const uuidv1 = require('uuid/v1');
const Utils = require('./utils');

class Server {

  constructor() {
  }

  /**
   * Initializes Config properties
   * @param config
   * @returns {Server}
   */
  config(config) {
    this.config = config || {};
    // this.logger config
    this.logger = require('./logger').init(_.get(this.config, 'logger', {}))

    return this;
  }

  /**
   * Connects to the message Broker
   * @param uri
   * @returns {Promise<any>}
   */
  async connect(uri) {
    this.uri = uri;

    this.conn = await amqp.connect(uri);
    this.logger.debug("Connected to broker")

    this.ch = await this.conn.createChannel();
    this.logger.debug("Channel created", this.ch)

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
      this.logger.error("`q` parameter must be a String");
      return;
    }

    if (options && !_.isObject(options)) {
      this.logger.error("`options` parameter must be an Object");
      return;
    }

    this.logger.debug("Registering procedure: %s", q);

    await this.ch.assertQueue(q, {durable: false});
    this.ch.prefetch(1);
    this.logger.debug(`[x] Awaiting RPC requests @${q}`);

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

    this.logger.debug(`[x] Consumer: Calling '%s' with args:`, method, msg);

    this.ch.consume(q.queue, function (msg) {
      if (msg.properties.correlationId == correlationId) {
        callback(Response.json(msg));
      }
    }, {noAck: true});


    this.ch.sendToQueue(method,
      Buffer.from(msg),
      {correlationId: correlationId, replyTo: q.queue});
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
    this.logger.debug("Send:Sent %s", Utils.stringOf(msg));
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
    this.logger.debug("[publish]: Sent %s", Utils.stringOf(msg));
    ch.close(err => {
      if (err) {
        this.logger.error("[publish]: Failed to close channel");
      }else{
        this.logger.debug("[publish]: Channel closed");
      }
    })

  }

  async subscribe(q, cb, options = {}) {
    options = _.merge({noAck: true}, options);

    this.logger.debug("[Subscribe:] Waiting for messages in %s", q);
    let self = this;
    await this.ch.consume(q, function (msg) {
      this.logger.debug("[Subscribe:] Received %s", msg.content.toString());
      cb(Response.json(msg));
      if(!options.noAck) self.ch.ack(msg);
    }, options);
  }


  /**
   * Parses json or yaml config file.
   * @returns {boolean}
   */
  parseConfigFile() {

    const validConfigTypes = ['json', 'yaml','yml'];
    let config;
    let configFileType = this.config.configFileType || 'json';
    if(validConfigTypes.indexOf(configFileType) < 0) {
      this.logger.error("Invalid `configFileType` attribute. Must be either `json`, `yaml` or empty");
      return false;
    }

    let configDir = path.join(process.cwd(), _.get(this.config, 'configDir') || 'config');

    if(configFileType === 'json') {
      config = require(path.join(configDir,'config'));
    }else{
      const yaml = require('js-yaml');
      const fs   = require('fs');

      try {
        config = yaml.safeLoad(fs.readFileSync(path.join(configDir,'config.yml'), 'utf8'));
      } catch (e) {
        console.error(e);
        process.exit(1);
      }
    }

    this.config = _.merge(this.config || {}, config);

  }


  /**
   * Sets up exchanges as defined in config.
   * @returns {Promise<boolean>}
   */
  async setupExchanges() {
    // Setup exchanges
    let exchanges = this.config.exchanges;
    if(!_.isObject(exchanges)) {
      this.logger.error("Invalid exchanges configuration. Exchanges must be an Object");
      return false;
    }


    this.logger.debug("----------------------------------------------------------")
    this.logger.debug("Asserting exchanges");
    this.logger.debug("----------------------------------------------------------")

    for(let key in exchanges) {
      let exchange_name = key;
      let type = exchanges[key].type;
      let options = exchanges[key].options;
      let args = exchanges[key].args; // ToDo implement RabbitMQ's `alternateExchange`
      let bindings = exchanges[key].queues;

      await this.ch.assertExchange(exchange_name, type, options);
      this.logger.debug("Assert exchange: %s", exchange_name);

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
          this.logger.debug("Assert queue: %s", q.queue);
          await this.ch.bindQueue(q.queue, exchange_name, queue.routingKey, queue.args)
          this.logger.debug("Bound queue `%s` to exchange `%s` using routingKey `%s`", q.queue, exchange_name, queue.routingKey || '');
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
      this.logger.error("Invalid queues configuration. Queues must be an Object");
      return false;
    }

    this.logger.debug("----------------------------------------------------------")
    this.logger.debug("Asserting orphaned queues");
    this.logger.debug("----------------------------------------------------------")

    for(let key in queues) {
      let queue_name = key;
      let options = queues[key].options;

      let q = await this.ch.assertQueue(queue_name, options);
      this.logger.debug("Assert queue: %s", q.queue);
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
      this.logger.error("Invalid subscriptions configuration. Subscriptions must be an Object");
      return false;
    }

    this.logger.debug("----------------------------------------------------------")
    this.logger.debug("Registering subscriptions");
    this.logger.debug("----------------------------------------------------------")

    for(let key in subs) {
      let queue_name = key;
      let handler = subs[key].handler;
      let handlerPath = handler.substr(0,handler.indexOf('.'));
      let handlerMethods = handler.substr(handler.indexOf('.') +1 );
      let handlerFile = require(path.join(process.cwd(),handlerPath));
      let options = subs[key].options;

      await this.subscribe(queue_name, this.toMethod(handlerFile, handlerMethods), options);
      this.logger.debug("Subscribed to queue: %s", queue_name);
    }
  }


  /**
   * Sets up procedures for RPC as defined in config.
   * @returns {Promise<void>}
   */
  async registerProcedures() {
    let procedures = this.config.procedures;
    if(!_.isObject(procedures)) {
      this.logger.error("Invalid procedures configuration. Procedures must be an Object");
      return false;
    }

    this.logger.debug("----------------------------------------------------------")
    this.logger.debug("Registering procedures");
    this.logger.debug("----------------------------------------------------------")

    for (let key in procedures) {
      let handler = procedures[key].handler;
      if (!handler) {
        this.logger.warn(`No handler defined for procedure "${key}"`)
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

    this.parseConfigFile();

    this.logger.debug("Loaded config");
    this.logger.debug("%j", this.config);

    await this.setupExchanges();
    await this.setupQueues();
    await this.registerProcedures();
    await this.registerSubscriptions();

    this.logger.debug("----------------------------------------------------------")
    this.logger.debug("Configuration completed");
    this.logger.debug("----------------------------------------------------------")
  }

  /**
   * Checks if queue exists.
   * Opens an ephemeral channel to do so.
   * @param q
   */
  async checkQueue(q, cb) {
    this.logger.debug("Checking if queue `%s` exists", q);
    this.conn.createChannel(function (err, ch) {
      if (err) {
        this.logger.error("Failed to create channel", err)
        return cb(err);
      }

      ch.checkQueue(q, cb);
    });
  }

}

module.exports = new Server();