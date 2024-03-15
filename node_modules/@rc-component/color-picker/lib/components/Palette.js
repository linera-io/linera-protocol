"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _react = _interopRequireDefault(require("react"));
var Palette = function Palette(_ref) {
  var children = _ref.children,
    style = _ref.style,
    prefixCls = _ref.prefixCls;
  return /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-palette"),
    style: (0, _objectSpread2.default)({
      position: 'relative'
    }, style)
  }, children);
};
var _default = exports.default = Palette;