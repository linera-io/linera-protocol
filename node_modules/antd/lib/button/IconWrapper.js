"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
const IconWrapper = /*#__PURE__*/(0, _react.forwardRef)((props, ref) => {
  const {
    className,
    style,
    children,
    prefixCls
  } = props;
  const iconWrapperCls = (0, _classnames.default)(`${prefixCls}-icon`, className);
  return /*#__PURE__*/_react.default.createElement("span", {
    ref: ref,
    className: iconWrapperCls,
    style: style
  }, children);
});
var _default = exports.default = IconWrapper;