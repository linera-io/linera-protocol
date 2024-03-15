"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
var _util = require("../util");
const ColorClear = _ref => {
  let {
    prefixCls,
    value,
    colorCleared,
    onChange
  } = _ref;
  const handleClick = () => {
    if (value && !colorCleared) {
      const hsba = value.toHsb();
      hsba.a = 0;
      const genColor = (0, _util.generateColor)(hsba);
      onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
    }
  };
  return /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-clear`,
    onClick: handleClick
  });
};
var _default = exports.default = ColorClear;