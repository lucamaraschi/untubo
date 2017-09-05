'use strict'

const t = require('tap')
const options = {
  'metadata.broker.list': '127.0.0.1:9092',
  'group.id': 'untubo-tests-1',
  'topic': 'untubo-tests'
}

const untubo = require('./')(options)
const expected = [
  { hello: 'world', i: 0 },
  { hello: 'world', i: 1 }
]

t.tearDown(untubo.stop.bind(untubo))
t.plan(expected.length)

untubo.pull((data, cb) => {
  // we must confirm the message before
  // any assertion, or we will crash badly
  cb()

  t.deepEqual(data, expected.shift())

  if (expected.lenth === 0) {
    untubo.stop()
  }
})

untubo.on('consumer', () => {
  expected.forEach((data) => untubo.push(data))
})
