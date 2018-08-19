"use strict";

const amqp = require('amqplib');
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
  async connect(uri) {
    this.uri = uri;

    this.conn = await amqp.connect(uri);
    logger.debug("Connected to broker")

    this.ch = await this.conn.createChannel();
    logger.debug("Channel created", this.ch)

    await this.includeConfig();

    return this;
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
  async includeConfig() {

    let includes = includeAll({
      dirname     :  path.join(process.cwd(),_.get(this.config,'configDir') || 'config'),
      filter      :  /(.+)\.js$/,
      optional    :  true
    });

    this.config = _.merge(this.config || {}, includes)

    const elligibleIncludes = ['queues','exchanges'];

    await Promise.all(elligibleIncludes.map(async (entity) => {
      switch(entity) {
        case "exchanges":
          await this.configExchanges();
          break;
        case "queues":
          await this.configQueues();
          break;
      }
    }));

    // Initialize procedures separately
    this.configProcedures();


    logger.debug("----------------------------------------------------------")
    logger.debug("Initialization completed");
    logger.debug("----------------------------------------------------------")


  }

  /**
   * Checks if queue exists.
   * Opens an ephemeral channel to do so.
   * @param q
   */
  async checkQueue(q, cb) {
    logger.debug("Checking if queue `%s` exists",q);
    this.conn.createChannel(function (err, ch) {
      if (err) {
        logger.error("Failed to create channel", err)
        return cb(err);
      }

      ch.checkQueue(q,cb);
    });
  }

  async configQueues() {
    for(let key in this.config.queues) {
      logger.debug("Creating queue => %s", key, JSON.stringify(this.config.queues[key]));
      let q = await this.ch.assertQueue(key, this.config.queues[key].options || {})
      logger.debug(`Created ${q.queue} => %s`,JSON.stringify(q))
    }
  }

  configProcedures() {
    // if(this.config.procedures && Object.keys(this.config.procedures).length) {
    //   for(let key in this.config.procedures) {
    //     if(!this.config.procedures[key].handler) {
    //       logger.warn(`No handler defined for procedure "${key}"`)
    //     }else{
    //       this.register(key, this.config.procedures[key].options || {}, this.config.procedures[key].handler);
    //     }
    //   }
    // }
  }

  async configExchanges() {

    if(!this.config.exchanges || !_.isObject(this.config.exchanges)) return false;

    for(let key in this.config.exchanges) {
      logger.debug("Creating exchange => %s", key, JSON.stringify(this.config.exchanges[key]));
      let ex = await this.ch.assertExchange(
        key,
        this.config.exchanges[key].type || "",
        this.config.exchanges[key].options || {}
        )
      logger.debug(`Created exchange ${ex.exchange} => %s`,JSON.stringify(ex))
    }

    await this.applyQueueBindings();
    
  }

  async applyQueueBindings() {

    // Check for and apply queue bindings
    let queues = this.getConfigQueues();
    let promises = []

    for(let key in queues) {
      let bindings = this.getConfigQueueBindings(queues[key]);
      bindings.forEach(bond => {
        promises.push(new Promise((resolve,reject) => {
          try{
            // Assert exchange in case it's not defined in config
            let availableExchange = this.config.exchanges[bond.exchange] || {};

            this.ch.assertExchange(
              bond.exchange, availableExchange.type,
              availableExchange.options,
              availableExchange.args
              ).then(ex => {
              this.ch.bindQueue(
                key,
                bond.exchange,
                bond.routingKey || '',
                bond.args
              )
              resolve({
                queue: key,
                bond: bond
              })
            })
          }catch(err) {
            reject(err);
          }

        }))
      })
    }

    await Promise.all(promises).then(bonds => {
      bonds.forEach(data => {
        logger.debug("Queue %s is bound to exchange %s", data.queue, data.bond.exchange)
      })

    }).catch(reason => {

      console.error(reason)
    });
  }

  getConfigQueues() {
    return this.config.queues && _.keys(this.config.queues).length ? this.config.queues : {};
  }

  getConfigQueueBindings(queue) {
    return queue.bindings && _.isArray(queue.bindings) ?  queue.bindings : [];
  }
}

module.exports = new Server();