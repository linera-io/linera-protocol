"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _LeftOutlined = _interopRequireDefault(require("@ant-design/icons/LeftOutlined"));
var _RightOutlined = _interopRequireDefault(require("@ant-design/icons/RightOutlined"));
var React = _interopRequireWildcard(require("react"));
var _button = _interopRequireDefault(require("../button"));
const Operation = props => {
  const {
    disabled,
    moveToLeft,
    moveToRight,
    leftArrowText = '',
    rightArrowText = '',
    leftActive,
    rightActive,
    className,
    style,
    direction,
    oneWay
  } = props;
  return /*#__PURE__*/React.createElement("div", {
    className: className,
    style: style
  }, /*#__PURE__*/React.createElement(_button.default, {
    type: "primary",
    size: "small",
    disabled: disabled || !rightActive,
    onClick: moveToRight,
    icon: direction !== 'rtl' ? /*#__PURE__*/React.createElement(_RightOutlined.default, null) : /*#__PURE__*/React.createElement(_LeftOutlined.default, null)
  }, rightArrowText), !oneWay && ( /*#__PURE__*/React.createElement(_button.default, {
    type: "primary",
    size: "small",
    disabled: disabled || !leftActive,
    onClick: moveToLeft,
    icon: direction !== 'rtl' ? /*#__PURE__*/React.createElement(_LeftOutlined.default, null) : /*#__PURE__*/React.createElement(_RightOutlined.default, null)
  }, leftArrowText)));
};
if (process.env.NODE_ENV !== 'production') {
  Operation.displayName = 'Operation';
}
var _default = exports.default = Operation;