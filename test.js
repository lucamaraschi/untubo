'use strict'

// test requires kafka to be running!!
const t = require('tap')
const options = {
  'metadata.broker.list': '127.0.0.1:9092',
  'group.id': 'kafka1',
  'topic': 'j2dlq',
  'key': 'testKey'
}
const expected = [
  { hello: 'world', count: 0 },
  { hello: 'world', count: 1 }
]
const untubo = require('./untubo')(options, function (err) {
  console.log('Kafka Error: ' + err)
  t.fail()
})

t.tearDown(function () {
  untubo.producer.stop()
  untubo.consumer.stop()
})

t.plan(expected.length)

untubo.consumer.poll(function (data, commit) {
  commit()
  t.deepEqual(data, expected.shift())
})

untubo.producer.push({hello: 'world', count: 0})
setTimeout(function () {
  untubo.producer.push({hello: 'world', count: 1})
}, 1000)
