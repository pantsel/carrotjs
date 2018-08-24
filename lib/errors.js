'use strict';

module.exports = {
  AmqpError: function AmqpError(message, extra) {
    Error.captureStackTrace(this, this.constructor);
    this.name = this.constructor.name;
    this.message = message;
    this.extra = extra;
  }
};

require('util').inherits(module.exports.AmqpError, Error);