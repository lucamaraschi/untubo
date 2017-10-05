'use strict'

const Kafka = require('node-rdkafka')

module.exports = function (opts, errCb) {
  const topic = opts.topic
  const key = opts.key
  var producer
  var ready = false

  delete opts.topic
  delete opts.key

  /**
   * initalize the producer
   */
  function init (readyCb) {
    producer = new Kafka.Producer(Object.assign(opts, {
      'dr_cb': true,
      'queue.buffering.max.ms': 0
    }))

    producer.connect({}, function (err) {
      if (err) { return errCb(err) }
    })

    producer.once('ready', function () {
      ready = true
      readyCb()
    })

    producer.on('event.error', errCb)
    producer.on('error', errCb)
  }

  function _push (payload) {
    try {
      producer.produce(topic, null, Buffer.from(payload), key, Date.now())
    } catch (err) {
      errCb(err)
    }
  }

  /**
   * push a message
   */
  function push (payload) {
    payload = JSON.stringify(payload)

    if (!ready) {
      init(function () {
        _push(payload)
      })
    } else {
      _push(payload)
    }
  }

  /**
   * disconnect the producer
   */
  function stop (cb) {
    producer.disconnect(cb)
  }

  return {
    init: init,
    push: push,
    stop: stop
  }
}
