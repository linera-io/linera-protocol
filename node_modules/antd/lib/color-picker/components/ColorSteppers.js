"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireWildcard(require("react"));
var _inputNumber = _interopRequireDefault(require("../../input-number"));
const ColorSteppers = _ref => {
  let {
    prefixCls,
    min = 0,
    max = 100,
    value,
    onChange,
    className,
    formatter
  } = _ref;
  const colorSteppersPrefixCls = `${prefixCls}-steppers`;
  const [stepValue, setStepValue] = (0, _react.useState)(value);
  // Update step value
  (0, _react.useEffect)(() => {
    if (!Number.isNaN(value)) {
      setStepValue(value);
    }
  }, [value]);
  return /*#__PURE__*/_react.default.createElement(_inputNumber.default, {
    className: (0, _classnames.default)(colorSteppersPrefixCls, className),
    min: min,
    max: max,
    value: stepValue,
    formatter: formatter,
    size: "small",
    onChange: step => {
      if (!value) {
        setStepValue(step || 0);
      }
      onChange === null || onChange === void 0 ? void 0 : onChange(step);
    }
  });
};
var _default = exports.default = ColorSteppers;