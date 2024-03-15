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
const ColorAlphaInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorAlphaInputPrefixCls = `${prefixCls}-alpha-input`;
  const [alphaValue, setAlphaValue] = (0, _react.useState)((0, _util.generateColor)(value || '#000'));
  // Update step value
  (0, _react.useEffect)(() => {
    if (value) {
      setAlphaValue(value);
    }
  }, [value]);
  const handleAlphaChange = step => {
    const hsba = alphaValue.toHsb();
    hsba.a = (step || 0) / 100;
    const genColor = (0, _util.generateColor)(hsba);
    if (!value) {
      setAlphaValue(genColor);
    }
    onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
  };
  return /*#__PURE__*/_react.default.createElement(_ColorSteppers.default, {
    value: (0, _util.getAlphaColor)(alphaValue),
    prefixCls: prefixCls,
    formatter: step => `${step}%`,
    className: colorAlphaInputPrefixCls,
    onChange: handleAlphaChange
  });
};
var _default = exports.default = ColorAlphaInput;