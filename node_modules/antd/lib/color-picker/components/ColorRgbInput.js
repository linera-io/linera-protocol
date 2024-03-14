"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _util = require("../util");
var _ColorSteppers = _interopRequireDefault(require("./ColorSteppers"));
const ColorRgbInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorRgbInputPrefixCls = `${prefixCls}-rgb-input`;
  const [rgbValue, setRgbValue] = (0, _react.useState)((0, _util.generateColor)(value || '#000'));
  // Update step value
  (0, _react.useEffect)(() => {
    if (value) {
      setRgbValue(value);
    }
  }, [value]);
  const handleRgbChange = (step, type) => {
    const rgb = rgbValue.toRgb();
    rgb[type] = step || 0;
    const genColor = (0, _util.generateColor)(rgb);
    if (!value) {
      setRgbValue(genColor);
    }
    onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
  };
  return /*#__PURE__*/_react.default.createElement("div", {
    className: colorRgbInputPrefixCls
  }, /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    max: 255,
    min: 0,
    value: Number(rgbValue.toRgb().r),
    prefixCls: prefixCls,
    className: colorRgbInputPrefixCls,
    onChange: step => handleRgbChange(Number(step), 'r')
  }), /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    max: 255,
    min: 0,
    value: Number(rgbValue.toRgb().g),
    prefixCls: prefixCls,
    className: colorRgbInputPrefixCls,
    onChange: step => handleRgbChange(Number(step), 'g')
  }), /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    max: 255,
    min: 0,
    value: Number(rgbValue.toRgb().b),
    prefixCls: prefixCls,
    className: colorRgbInputPrefixCls,
    onChange: step => handleRgbChange(Number(step), 'b')
  }));
};
var _default = exports.default = ColorRgbInput;