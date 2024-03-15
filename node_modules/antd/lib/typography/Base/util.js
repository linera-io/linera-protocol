"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getNode = getNode;
exports.toList = toList;
function toList(val) {
  if (val === false) {
    return [false, false];
  }
  return Array.isArray(val) ? val : [val];
}
function getNode(dom, defaultNode, needDom) {
  if (dom === true || dom === undefined) {
    return defaultNode;
  }
  return dom || needDom && defaultNode;
}