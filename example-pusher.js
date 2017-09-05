'use strict'

const options = {
  'metadata.broker.list': '127.0.0.1:9092',
  'group.id': 'kafka1',
  'topic': 'test',
  key: 'testKey'
}

const kafkesque = require('./untubo')(options)

var count = 0
var interval = setInterval(function () {
  kafkesque.push({hello: 'world', count})
  console.log('sent', count)
  count++
}, 500)

process.once('SIGINT', function () {
  clearInterval(interval)
  console.log('closing')
  kafkesque.stop(() => {
    console.log('closed')
  })
})
