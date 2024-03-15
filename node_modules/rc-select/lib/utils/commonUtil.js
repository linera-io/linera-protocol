"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getTitle = getTitle;
exports.hasValue = hasValue;
exports.isClient = exports.isBrowserClient = void 0;
exports.isComboNoValue = isComboNoValue;
exports.toArray = toArray;
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
function toArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  return value !== undefined ? [value] : [];
}
var isClient = exports.isClient = typeof window !== 'undefined' && window.document && window.document.documentElement;

/** Is client side and not jsdom */
var isBrowserClient = exports.isBrowserClient = process.env.NODE_ENV !== 'test' && isClient;
function hasValue(value) {
  return value !== undefined && value !== null;
}

/** combo mode no value judgment function */
function isComboNoValue(value) {
  return !value && value !== 0;
}
function isTitleType(title) {
  return ['string', 'number'].includes((0, _typeof2.default)(title));
}
function getTitle(item) {
  var title = undefined;
  if (item) {
    if (isTitleType(item.title)) {
      title = item.title.toString();
    } else if (isTitleType(item.label)) {
      title = item.label.toString();
    }
  }
  return title;
}