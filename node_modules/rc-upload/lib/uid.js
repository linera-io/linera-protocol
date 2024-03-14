"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = uid;
var now = +new Date();
var index = 0;
function uid() {
  // eslint-disable-next-line no-plusplus
  return "rc-upload-".concat(now, "-").concat(++index);
}