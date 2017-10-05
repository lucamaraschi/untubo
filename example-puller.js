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

untubo.consumer.poll(function (data, commit) {
  console.log('DATA: ', data)
  commit()
})

process.once('SIGINT', function () {
  console.log('stopping...')
  untubo.consumer.stop(function () {
    console.log('stopped')
  })
})
