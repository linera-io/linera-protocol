"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useColumnIcons;
var React = _interopRequireWildcard(require("react"));
var _LeftOutlined = _interopRequireDefault(require("@ant-design/icons/LeftOutlined"));
var _LoadingOutlined = _interopRequireDefault(require("@ant-design/icons/LoadingOutlined"));
var _RightOutlined = _interopRequireDefault(require("@ant-design/icons/RightOutlined"));
function useColumnIcons(prefixCls, rtl, expandIcon) {
  let mergedExpandIcon = expandIcon;
  if (!expandIcon) {
    mergedExpandIcon = rtl ? /*#__PURE__*/React.createElement(_LeftOutlined.default, null) : /*#__PURE__*/React.createElement(_RightOutlined.default, null);
  }
  const loadingIcon = /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-menu-item-loading-icon`
  }, /*#__PURE__*/React.createElement(_LoadingOutlined.default, {
    spin: true
  }));
  return [mergedExpandIcon, loadingIcon];
}