"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.cloneElement = cloneElement;
exports.isFragment = isFragment;
exports.replaceElement = void 0;
var _react = _interopRequireDefault(require("react"));
function isFragment(child) {
  return child && /*#__PURE__*/_react.default.isValidElement(child) && child.type === _react.default.Fragment;
}
const replaceElement = (element, replacement, props) => {
  if (! /*#__PURE__*/_react.default.isValidElement(element)) {
    return replacement;
  }
  return /*#__PURE__*/_react.default.cloneElement(element, typeof props === 'function' ? props(element.props || {}) : props);
};
exports.replaceElement = replaceElement;
function cloneElement(element, props) {
  return replaceElement(element, element, props);
}