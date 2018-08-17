"use.strict"

module.exports = {
  stringOf: (msg) => {
    try {
      msg  = JSON.stringify(msg);
    }catch (e) {
      msg = msg.toString();
    }
    return msg;
  }
}