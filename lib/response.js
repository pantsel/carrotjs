"use strict";

const _ = require('lodash');

module.exports = {
  json: (msg) => {
    let response;
    msg = msg.content || msg;

    try {
      response = JSON.parse(msg.toString());
    }catch(err) {
      response = msg.toString();
    }

    return response;
  },

  string: (response) => {
    try {
      response  = JSON.stringify(response);
    }catch (e) {
      response = response.toString();
    }
    return response;
  }
}