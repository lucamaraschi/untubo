'use strict'

module.exports = function (opts, errCb) {
  return {
    producer: require('./producer')(Object.assign({}, opts), errCb),
    consumer: require('./consumer')(Object.assign({}, opts), errCb)
  }
}
