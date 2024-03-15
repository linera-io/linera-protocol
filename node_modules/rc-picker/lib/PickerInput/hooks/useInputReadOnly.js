"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useInputReadOnly;
function useInputReadOnly(formatList, inputReadOnly, multiple) {
  if (typeof formatList[0] === 'function' || multiple) {
    return true;
  }
  return inputReadOnly;
}