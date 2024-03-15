"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _colorPicker = _interopRequireDefault(require("@rc-component/color-picker"));
var _react = _interopRequireWildcard(require("react"));
var _context = require("../context");
var _ColorClear = _interopRequireDefault(require("./ColorClear"));
var _ColorInput = _interopRequireDefault(require("./ColorInput"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const PanelPicker = () => {
  const _a = (0, _react.useContext)(_context.PanelPickerContext),
    {
      prefixCls,
      colorCleared,
      allowClear,
      value,
      disabledAlpha,
      onChange,
      onClear,
      onChangeComplete
    } = _a,
    injectProps = __rest(_a, ["prefixCls", "colorCleared", "allowClear", "value", "disabledAlpha", "onChange", "onClear", "onChangeComplete"]);
  return /*#__PURE__*/_react.default.createElement(_react.default.Fragment, null, allowClear && ( /*#__PURE__*/_react.default.createElement(_ColorClear.default, Object.assign({
    prefixCls: prefixCls,
    value: value,
    colorCleared: colorCleared,
    onChange: clearColor => {
      onChange === null || onChange === void 0 ? void 0 : onChange(clearColor);
      onClear === null || onClear === void 0 ? void 0 : onClear();
    }
  }, injectProps))), /*#__PURE__*/_react.default.createElement(_colorPicker.default, {
    prefixCls: prefixCls,
    value: value === null || value === void 0 ? void 0 : value.toHsb(),
    disabledAlpha: disabledAlpha,
    onChange: (colorValue, type) => onChange === null || onChange === void 0 ? void 0 : onChange(colorValue, type, true),
    onChangeComplete: onChangeComplete
  }), /*#__PURE__*/_react.default.createElement(_ColorInput.default, Object.assign({
    value: value,
    onChange: onChange,
    prefixCls: prefixCls,
    disabledAlpha: disabledAlpha
  }, injectProps)));
};
var _default = exports.default = PanelPicker;