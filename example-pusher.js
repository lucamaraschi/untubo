'use strict'

const options = {
  'metadata.broker.list': '127.0.0.1:9092',
  'group.id': 'kafka1',
  'topic': 'j2dlq',
  'key': 'testKey'
}


const untubo = require('./untubo')(options, function (err) {
  console.log('Kafka Error: ' + err)
})
var interval
var count = 0


untubo.producer.init(function () {
  interval = setInterval(function () {
    console.log('pushing...')
    untubo.producer.push({hello: 'world', count})
    console.log('sent', count)
    count++
  }, 1000)
})


process.once('SIGINT', function () {
  clearInterval(interval)
  console.log('stopping...')
  untubo.producer.stop(function () {
    console.log('stopped')
  })
})

