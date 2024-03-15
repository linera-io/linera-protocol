"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = getFixScaleEleTransPosition;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _css = require("rc-util/lib/Dom/css");
function fixPoint(key, start, width, clientWidth) {
  var startAddWidth = start + width;
  var offsetStart = (width - clientWidth) / 2;
  if (width > clientWidth) {
    if (start > 0) {
      return (0, _defineProperty2.default)({}, key, offsetStart);
    }
    if (start < 0 && startAddWidth < clientWidth) {
      return (0, _defineProperty2.default)({}, key, -offsetStart);
    }
  } else if (start < 0 || startAddWidth > clientWidth) {
    return (0, _defineProperty2.default)({}, key, start < 0 ? offsetStart : -offsetStart);
  }
  return {};
}

/**
 * Fix positon x,y point when
 *
 * Ele width && height < client
 * - Back origin
 *
 * - Ele width | height > clientWidth | clientHeight
 * - left | top > 0 -> Back 0
 * - left | top + width | height < clientWidth | clientHeight -> Back left | top + width | height === clientWidth | clientHeight
 *
 * Regardless of other
 */
function getFixScaleEleTransPosition(width, height, left, top) {
  var _getClientSize = (0, _css.getClientSize)(),
    clientWidth = _getClientSize.width,
    clientHeight = _getClientSize.height;
  var fixPos = null;
  if (width <= clientWidth && height <= clientHeight) {
    fixPos = {
      x: 0,
      y: 0
    };
  } else if (width > clientWidth || height > clientHeight) {
    fixPos = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, fixPoint('x', left, width, clientWidth)), fixPoint('y', top, height, clientHeight));
  }
  return fixPos;
}