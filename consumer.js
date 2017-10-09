'use strict'

const Kafka = require('node-rdkafka')

module.exports = function (opts, errCb) {
  const topic = opts.topic
  var consumer
  var ready = false
  var handlers = false

  delete opts.topic
  delete opts.key

  /**
   * begin polling, as data is reveived call the dataCb for each message.
   * the client of this library is responsible for calling the commit callback
   * function. In the case of an error, no data will be emitted and the message
   * will be comitted.
   */
  function _poll (dataCb, forever) {
    if (!ready) { return errCb('Error: consumer not ready, ensure that init is called before attempting poll') }

    if (!handlers) {
      consumer.on('data', function (msg) {
        var json
        try {
          json = JSON.parse(msg.value)
        } catch (err) {
          consumer.commitMessage(msg)
          return errCb(err)
        }

        dataCb(json, function () {
          consumer.commitMessage(msg)
        })
      })
      handlers = true
    }

    if (forever) {
      consumer.consume()
    } else {
      consumer.consume(1)
    }
  }

  /**
   * initalize the consumer for polling in flow mode
   */
  function init (readyCb) {
    consumer = new Kafka.KafkaConsumer(Object.assign(opts, {
      'fetch.wait.max.ms': 10,
      'fetch.error.backoff.ms': 50
    }), {})

    consumer.connect({}, function (err) {
      if (err) { return errCb(err) }
    })

    consumer.once('ready', function () {
      ready = true
      consumer.subscribe([topic])
      readyCb()
    })
    consumer.on('event.error', errCb)
    consumer.on('error', errCb)
  }

  function poll (dataCb, forever) {
    if (!ready) {
      init(function () {
        _poll(dataCb, forever)
      })
    } else {
      _poll(dataCb, forever)
    }
  }

  /**
   * stop all poll activites and discounect the client
   */
  function stop (cb) {
    if (consumer) {
      consumer.unsubscribe()
      consumer.disconnect(cb)
    }
  }

  return {
    poll: poll,
    stop: stop
  }
}
