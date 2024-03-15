"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _FileTextOutlined = _interopRequireDefault(require("@ant-design/icons/FileTextOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
const FloatButtonContent = props => {
  const {
    icon,
    description,
    prefixCls,
    className
  } = props;
  const defaultElement = /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-icon`
  }, /*#__PURE__*/_react.default.createElement(_FileTextOutlined.default, null));
  return /*#__PURE__*/_react.default.createElement("div", {
    onClick: props.onClick,
    onFocus: props.onFocus,
    onMouseEnter: props.onMouseEnter,
    onMouseLeave: props.onMouseLeave,
    className: (0, _classnames.default)(className, `${prefixCls}-content`)
  }, icon || description ? ( /*#__PURE__*/_react.default.createElement(_react.default.Fragment, null, icon && /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-icon`
  }, icon), description && /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-description`
  }, description))) : defaultElement);
};
var _default = exports.default = /*#__PURE__*/(0, _react.memo)(FloatButtonContent);