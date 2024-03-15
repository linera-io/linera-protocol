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
const ColorHsbInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorHsbInputPrefixCls = `${prefixCls}-hsb-input`;
  const [hsbValue, setHsbValue] = (0, _react.useState)((0, _util.generateColor)(value || '#000'));
  // Update step value
  (0, _react.useEffect)(() => {
    if (value) {
      setHsbValue(value);
    }
  }, [value]);
  const handleHsbChange = (step, type) => {
    const hsb = hsbValue.toHsb();
    hsb[type] = type === 'h' ? step : (step || 0) / 100;
    const genColor = (0, _util.generateColor)(hsb);
    if (!value) {
      setHsbValue(genColor);
    }
    onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
  };
  return /*#__PURE__*/_react.default.createElement("div", {
    className: colorHsbInputPrefixCls
  }, /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    max: 360,
    min: 0,
    value: Number(hsbValue.toHsb().h),
    prefixCls: prefixCls,
    className: colorHsbInputPrefixCls,
    formatter: step => (0, _util.getRoundNumber)(step || 0).toString(),
    onChange: step => handleHsbChange(Number(step), 'h')
  }), /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    max: 100,
    min: 0,
    value: Number(hsbValue.toHsb().s) * 100,
    prefixCls: prefixCls,
    className: colorHsbInputPrefixCls,
    formatter: step => `${(0, _util.getRoundNumber)(step || 0)}%`,
    onChange: step => handleHsbChange(Number(step), 's')
  }), /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    max: 100,
    min: 0,
    value: Number(hsbValue.toHsb().b) * 100,
    prefixCls: prefixCls,
    className: colorHsbInputPrefixCls,
    formatter: step => `${(0, _util.getRoundNumber)(step || 0)}%`,
    onChange: step => handleHsbChange(Number(step), 'b')
  }));
};
var _default = exports.default = ColorHsbInput;