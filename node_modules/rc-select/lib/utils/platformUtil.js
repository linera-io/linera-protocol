"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isPlatformMac = isPlatformMac;
/* istanbul ignore file */
function isPlatformMac() {
  return /(mac\sos|macintosh)/i.test(navigator.appVersion);
}