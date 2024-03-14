"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = channelUpdate;
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
function channelUpdate(callback) {
  if (typeof MessageChannel === 'undefined') {
    (0, _raf.default)(callback);
  } else {
    var channel = new MessageChannel();
    channel.port1.onmessage = function () {
      return callback();
    };
    channel.port2.postMessage(undefined);
  }
}