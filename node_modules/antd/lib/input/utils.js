"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.hasPrefixSuffix = hasPrefixSuffix;
// eslint-disable-next-line import/prefer-default-export
function hasPrefixSuffix(props) {
  return !!(props.prefix || props.suffix || props.allowClear || props.showCount);
}