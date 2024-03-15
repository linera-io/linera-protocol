"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _colorPicker = require("@rc-component/color-picker");
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireWildcard(require("react"));
var _util = require("../util");
var _ColorClear = _interopRequireDefault(require("./ColorClear"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const ColorTrigger = /*#__PURE__*/(0, _react.forwardRef)((props, ref) => {
  const {
      color,
      prefixCls,
      open,
      colorCleared,
      disabled,
      format,
      className,
      showText
    } = props,
    rest = __rest(props, ["color", "prefixCls", "open", "colorCleared", "disabled", "format", "className", "showText"]);
  const colorTriggerPrefixCls = `${prefixCls}-trigger`;
  const containerNode = (0, _react.useMemo)(() => colorCleared ? ( /*#__PURE__*/_react.default.createElement(_ColorClear.default, {
    prefixCls: prefixCls
  })) : ( /*#__PURE__*/_react.default.createElement(_colorPicker.ColorBlock, {
    prefixCls: prefixCls,
    color: color.toRgbString()
  })), [color, colorCleared, prefixCls]);
  const genColorString = () => {
    const hexString = color.toHexString().toUpperCase();
    const alpha = (0, _util.getAlphaColor)(color);
    switch (format) {
      case 'rgb':
        return color.toRgbString();
      case 'hsb':
        return color.toHsbString();
      case 'hex':
      default:
        return alpha < 100 ? `${hexString.slice(0, 7)},${alpha}%` : hexString;
    }
  };
  const renderText = () => {
    if (typeof showText === 'function') {
      return showText(color);
    }
    if (showText) {
      return genColorString();
    }
  };
  return /*#__PURE__*/_react.default.createElement("div", Object.assign({
    ref: ref,
    className: (0, _classnames.default)(colorTriggerPrefixCls, className, {
      [`${colorTriggerPrefixCls}-active`]: open,
      [`${colorTriggerPrefixCls}-disabled`]: disabled
    })
  }, rest), containerNode, showText && /*#__PURE__*/_react.default.createElement("div", {
    className: `${colorTriggerPrefixCls}-text`
  }, renderText()));
});
var _default = exports.default = ColorTrigger;