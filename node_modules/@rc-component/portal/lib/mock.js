"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.inline = void 0;
exports.inlineMock = inlineMock;
var inline = false;
exports.inline = inline;
function inlineMock(nextInline) {
  if (typeof nextInline === 'boolean') {
    exports.inline = inline = nextInline;
  }
  return inline;
}