"use strict";

const amqp = require('amqplib/callback_api');
const logger = require('./logger');
const _ = require('lodash');
const uuidv1 = require('uuid/v1');


class ISC {


  constructor(uri) {
    this.uri = uri;
    this.queues = [];
  }


  async connect() {
    let self = this;
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

          resolve(ch);

        });


      });
    })
  }


  addUri(uri, callback) {
    let self = this;
    let q = uri;
    this.ch.assertQueue(q, {durable: false});
    this.ch.prefetch(1);
    console.log(' [x] Awaiting RPC requests');

    this.ch.consume(q, function reply(msg) {
      var n = parseInt(msg.content.toString());

      console.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", n)

      console.log(" [.] fib(%d)", n);

      let r = self.fibonacci(n);

      self.ch.sendToQueue(msg.properties.replyTo,
        new Buffer(r.toString()),
        {correlationId: msg.properties.correlationId});

      self.ch.ack(msg);
    })
  }

  listenToRpcQueue() {
    let self = this;
    let q = 'rpc_queue';
    this.ch.assertQueue(q, {durable: false});
    this.ch.prefetch(1);
    console.log(' [x] Awaiting RPC requests');
    this.ch.consume(q, function reply(msg) {
      var n = parseInt(msg.content.toString());

      console.log(" [.] fib(%d)", n);

      let r = self.fibonacci(n);

      self.ch.sendToQueue(msg.properties.replyTo,
        new Buffer(r.toString()),
        {correlationId: msg.properties.correlationId});

      self.ch.ack(msg);
    })
  }

  rpc() {
    let self = this;
    this.conn.createChannel(function (err, ch) {
      self.consumerChannel = ch;
      ch.assertQueue('', {exclusive: true}, function (err, q) {
        var correlationId = uuidv1();
        var num = 123;

        console.log(' [x] Requesting fib(%d)', num);

        ch.consume(q.queue, function (msg) {
          if (msg.properties.correlationId == corr) {
            console.log(' [.] Got %s', msg.content.toString());
          }
        }, {noAck: true});

        ch.sendToQueue('rpc_queue',
          Buffer.from(num.toString()),
          {correlationId: correlationId, replyTo: q.queue});
      });
    })
  }


  assertQueues(qs) {
    qs.forEach(q => this.assertQueue(q));
  }

  assertQueue(q) {
    if (!this.ch) {
      logger.error("Channel is not created. You need to call `connect` and wait for the promise to resolve");
      process.exit(1);
    }
    this.ch.assertQueue(q.name, q.options);
    this.queues = this.queues.concat(q);
    logger.debug(`Queue ${q.name} added`, q.options);
  }


  /**
   * Test function
   * @param n
   * @returns {*}
   */
  fibonacci(n) {
    if (n == 0 || n == 1)
      return n;
    else
      return this.fibonacci(n - 1) + this.fibonacci(n - 2);
  }


  send(q, data) {
    if (!this.ch) {
      logger.error("Channel is not created. You need to call `connect` and wait for the promise to resolve");
      process.exit(1);
    }

    if (!_.isObject(q)) {
      logger.error("Queue must be an object with `name` and `options` attributes");
      return;
    }

    let msg = _.isString(data) ? data : JSON.stringify(data);

    // Assert queue if not exists
    // const exists = _.find(this.queues, (_q) => q.name === _q.name);
    // console.log("!!!!!!!!!!!!!!!!!!!", this.ch)
    // if(!exists) this.assertQueue(q);
    this.assertQueue(q); // Use assertQueue in case it does not exist
    this.ch.sendToQueue(q.name, Buffer.from(msg));

    logger.debug(`Sent %s to %s`, msg, q.name);
  }


}

module.exports = ISC;
