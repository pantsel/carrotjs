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

let a = new ISC('amqp://hxjhjrac:0R-cMo8zwkXWi5tyLq59UHyNQv0QKh3t@crocodile.rmq.cloudamqp.com/hxjhjrac\n')
a.connect().then(channel => {

  // a.send({
  //   name: 'q1',
  //   options: {
  //     durable: false
  //   }
  // },{
  //   msg: 'test'
  // })

  // a.listenToRpcQueue();

  // setTimeout(() => {
  //   a.rpc();
  // }, 1000)
  
  
  a.addUri('rpc_queue', function (channel, message) {
    
  })

  // setTimeout(() => {
  //   a.send({
  //     name: 'q1',
  //     options: {
  //       durable: false
  //     }
  //   },{
  //     msg: 'test1'
  //   })
  //
  //
  //   a.send({
  //     name: 'q2',
  //     options: {
  //       durable: false
  //     }
  //   },{
  //     msg: 'test1'
  //   })
  // },1000)
})


//
// ISC.connect('localhost').then(data => {
//
//   console.log(data)
//
// })