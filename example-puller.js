'use strict'

const options = {
  'metadata.broker.list': '127.0.0.1:9092',
  'group.id': 'kafka1',
  'topic': 'test',
  'key': 'testKey'
}

const kafkesque = require('./untubo')(options)

kafkesque.pull((data, cb) => {
  try {
    console.log('DATA: ', data)
  } catch (err) {
    console.log(err)
  }
  cb()
})

process.once('SIGINT', function () {
  console.log('closing')
  kafkesque.stop(function () {
    console.log('closed')
  })
})
