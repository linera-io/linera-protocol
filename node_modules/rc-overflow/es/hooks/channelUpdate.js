import raf from "rc-util/es/raf";
export default function channelUpdate(callback) {
  if (typeof MessageChannel === 'undefined') {
    raf(callback);
  } else {
    var channel = new MessageChannel();
    channel.port1.onmessage = function () {
      return callback();
    };
    channel.port2.postMessage(undefined);
  }
}