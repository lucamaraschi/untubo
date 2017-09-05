'use strict'

const Kafka = require('node-rdkafka')
const { inherits } = require('util')
const { EventEmitter } = require('events')

/*
const Joi = require('joi')
const schema = Joi.object().keys({
    'client.id': Joi.string().alphanum().required(),
    'metadata.broker.list': Joi.string().alphanum().required(),
    'compression.codec': Joi.string().alphanum(),
    'retry.backoff.ms': Joi.integer().min(0),
    'message.send.max.retries': Joi.integer().min(0),
    'socket.keepalive.enable': Joi.boolean(),
    'queue.buffering.max.messages': Joi.integer().min(0),
    'queue.buffering.max.ms': Joi.integer().min(0),
    'batch.num.messages': Joi.integer().min(0),
    'dr_cb': Joi.boolean()
})
*/

function Untubo (opts) {
  opts = opts || {}

  if (!(this instanceof Untubo)) {
    const instance = new Untubo(opts)
    return instance
  }

  this.isProducerReady = false
  this.isConsumerReady = false

  this._opts = opts
  this.topic = opts.topic
  this.key = opts.key

  delete opts.topic
  delete opts.key
}

inherits(Untubo, EventEmitter)

Untubo.prototype._initProducer = function () {
  if (this.producer) {
    return
  }

  this.producer = new Kafka.Producer(Object.assign(this._opts, {
    'dr_cb': true,
    'queue.buffering.max.ms': 0
  }))

  this.producer.connect({}, (err) => {
    if (err) {
      this.emit('error', err)
    }
  })

  this.producer.once('ready', () => {
    this.isProducerReady = true
    this.emit('producer')
  })
}

Untubo.prototype._initConsumer = function () {
  if (this.consumer) {
    return
  }

  this.consumer = new Kafka.KafkaConsumer(Object.assign(this._opts, {
    'fetch.wait.max.ms': 10,
    'fetch.error.backoff.ms': 50
  }), {})

  this.consumer.connect({}, (err) => {
    if (err) {
      this.emit('error', err)
      return
    }
    this.consumer.consume()
    this.consumer.subscribe([this.topic])
  })

  this.consumer.once('ready', () => {
    this.isConsumerReady = true
    this.emit('consumer')
  })
}

Untubo.prototype.push = function (payload) {
  if (!this.isProducerReady) {
    this._initProducer()
    this.once('producer', this.push.bind(this, payload))
    return
  }

  // this will throw if it cannot be stringified
  payload = JSON.stringify(payload)

  try {
    this.producer.produce(this.topic, null, Buffer.from(payload), this.key, Date.now())
  } catch (err) {
    // FIXME this needs to be more specific
    this.emit('error', err)
  }
}

Untubo.prototype.pull = function (onData) {
  if (!this.isConsumerReady) {
    this._initConsumer()
    this.once('consumer', this.pull.bind(this, onData))
    return
  }

  this.consumer.on('data', (msg) => {
    const done = (err) => {
      if (!err) {
        this.consumer.commitMessage(msg)
      } else {
        this.emit('pullError', msg, err)
      }
    }

    var json
    try {
      json = JSON.parse(msg.value)
    } catch (err) {
      return done(err)
    }

    onData(json, done)
  })
}

Untubo.prototype.stop = function (cb) {
  var count = 0
  if (this.producer) {
    count++
    this.producer.disconnect(done)
  }

  if (this.consumer) {
    count++
    this.consumer.unsubscribe()
    this.consumer.disconnect(done)
  }

  function done () {
    count--
    if (count === 0 && cb) {
      cb()
    }
  }
}

module.exports = Untubo
