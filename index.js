const ISC = require('./lib/isc');
let qs = [
  {
    name: 'q1',
    options: {
      durable: false
    }
  },
  {
    name: 'q2',
    options: {
      durable: false
    }
  },
  {
    name: 'q3',
    options: {
      durable: false
    }
  }
];

const Server = require('./lib/server');

Server.connect('amqp://hxjhjrac:0R-cMo8zwkXWi5tyLq59UHyNQv0QKh3t@crocodile.rmq.cloudamqp.com/hxjhjrac')
  .then(server => {
    server.register("test", {}, (data, next) => {

      console.log("------------------------------", data)

      return next({
        message: "OK"
      })

    })
  })


setTimeout(()=> {
  const Consumer = require('./lib/consumer');

  let _consumer = new Consumer('amqp://hxjhjrac:0R-cMo8zwkXWi5tyLq59UHyNQv0QKh3t@crocodile.rmq.cloudamqp.com/hxjhjrac');
  _consumer.connect().then(consumer => {
    consumer.call("test",{text: 'Hello'}, (res) => {
      console.log("Consumer: Got response", res)
    })
  })
}, 1000)

