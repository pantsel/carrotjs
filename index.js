const brokerUri = `amqp://hxjhjrac:0R-cMo8zwkXWi5tyLq59UHyNQv0QKh3t@crocodile.rmq.cloudamqp.com/hxjhjrac`;
const Server = require('./lib/server');
new Server()
  .connect(brokerUri)
  .then(server => {

    // Register remote procedures
    server.register("add", {}, (args, reply) => {
      let result = args[0] + args[1];
      return reply(result);
    })

    server.register("slugify", {}, (string, reply) => {
      let result = string.split(" ").join("-").toLowerCase();
      return reply(result);
    })
  })


const Consumer = require('./lib/consumer');

new Consumer()
  .connect(brokerUri)
  .then(consumer => {

    // Call remote procedures
    consumer.call("add", [3, 5], (res) => {
      console.log("Consumer:add: Got response", res)
    })

    consumer.call("slugify", "Hello there you", (res) => {
      console.log("Consumer:slugify: Got response", res)
    })
  })

