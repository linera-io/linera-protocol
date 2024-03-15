"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getClearIcon = getClearIcon;
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _react = _interopRequireDefault(require("react"));
function getClearIcon(prefixCls, allowClear, clearIcon) {
  var mergedClearIcon = (0, _typeof2.default)(allowClear) === "object" ? allowClear.clearIcon : clearIcon;
  return mergedClearIcon || /*#__PURE__*/_react.default.createElement("span", {
    className: "".concat(prefixCls, "-clear-btn")
  });
}