"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.nextSlice = nextSlice;
function nextSlice(callback) {
  /* istanbul ignore next */
  Promise.resolve().then(callback);
}