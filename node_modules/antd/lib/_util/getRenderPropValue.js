"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getRenderPropValue = void 0;
const getRenderPropValue = propValue => {
  if (!propValue) {
    return null;
  }
  return typeof propValue === 'function' ? propValue() : propValue;
};
exports.getRenderPropValue = getRenderPropValue;