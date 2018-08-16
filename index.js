const brokerUri = `amqp://hxjhjrac:0R-cMo8zwkXWi5tyLq59UHyNQv0QKh3t@crocodile.rmq.cloudamqp.com/hxjhjrac`;
const Server = require('./lib/server');
new Server()
  .connect(brokerUri)
  .then(server => {

    const procedures = {
      "add": {
        options: {durable: false},
        handler: onAdd
      },
      "divide": {
        options: {durable: false},
        handler: onDivide
      },
      "slugify": {
        handler: onSlugify
      }
    };

    function onAdd(args, reply) {
      let result = args[0] + args[1];
      return reply(result);
    }

    function onDivide(args, reply) {
      let result = args.divisor / args.divident;
      return reply({
        result: result
      });
    }

    function onSlugify(args, reply) {
      let result = args.split(" ").join("-").toLowerCase();
      return reply(result);
    }

    // Register remote procedures
    for(let key in procedures) {
      server.register(key, procedures[key].options || {}, procedures[key].handler);
    }
  })


const Consumer = require('./lib/consumer');

new Consumer()
  .connect(brokerUri)
  .then(consumer => {

    consumer.call("add", [3, 5], (res) => {
      console.log("Consumer:add: Got response", res)
    })

    consumer.call("divide", {
      divisor: 100,
      divident: 5
    }, (res) => {
      console.log("Consumer:divide: Got response", res)
    })

    consumer.call("slugify", "Hello there you", (res) => {
      console.log("Consumer:slugify: Got response", res)
    })
  })

