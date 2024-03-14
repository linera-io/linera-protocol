"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _LoadingOutlined = _interopRequireDefault(require("@ant-design/icons/LoadingOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = _interopRequireDefault(require("rc-motion"));
var _react = _interopRequireWildcard(require("react"));
var _IconWrapper = _interopRequireDefault(require("./IconWrapper"));
const InnerLoadingIcon = /*#__PURE__*/(0, _react.forwardRef)((_ref, ref) => {
  let {
    prefixCls,
    className,
    style,
    iconClassName
  } = _ref;
  const mergedIconCls = (0, _classnames.default)(`${prefixCls}-loading-icon`, className);
  return /*#__PURE__*/_react.default.createElement(_IconWrapper.default, {
    prefixCls: prefixCls,
    className: mergedIconCls,
    style: style,
    ref: ref
  }, /*#__PURE__*/_react.default.createElement(_LoadingOutlined.default, {
    className: iconClassName
  }));
});
const getCollapsedWidth = () => ({
  width: 0,
  opacity: 0,
  transform: 'scale(0)'
});
const getRealWidth = node => ({
  width: node.scrollWidth,
  opacity: 1,
  transform: 'scale(1)'
});
const LoadingIcon = props => {
  const {
    prefixCls,
    loading,
    existIcon,
    className,
    style
  } = props;
  const visible = !!loading;
  if (existIcon) {
    return /*#__PURE__*/_react.default.createElement(InnerLoadingIcon, {
      prefixCls: prefixCls,
      className: className,
      style: style
    });
  }
  return /*#__PURE__*/_react.default.createElement(_rcMotion.default, {
    visible: visible,
    // We do not really use this motionName
    motionName: `${prefixCls}-loading-icon-motion`,
    motionLeave: visible,
    removeOnLeave: true,
    onAppearStart: getCollapsedWidth,
    onAppearActive: getRealWidth,
    onEnterStart: getCollapsedWidth,
    onEnterActive: getRealWidth,
    onLeaveStart: getRealWidth,
    onLeaveActive: getCollapsedWidth
  }, (_ref2, ref) => {
    let {
      className: motionCls,
      style: motionStyle
    } = _ref2;
    return /*#__PURE__*/_react.default.createElement(InnerLoadingIcon, {
      prefixCls: prefixCls,
      className: className,
      style: Object.assign(Object.assign({}, style), motionStyle),
      ref: ref,
      iconClassName: motionCls
    });
  });
};
var _default = exports.default = LoadingIcon;