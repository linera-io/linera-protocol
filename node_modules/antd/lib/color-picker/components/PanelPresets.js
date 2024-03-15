"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _context = require("../context");
var _ColorPresets = _interopRequireDefault(require("./ColorPresets"));
const PanelPresets = () => {
  const {
    prefixCls,
    value,
    presets,
    onChange
  } = (0, _react.useContext)(_context.PanelPresetsContext);
  return Array.isArray(presets) ? ( /*#__PURE__*/_react.default.createElement(_ColorPresets.default, {
    value: value,
    presets: presets,
    prefixCls: prefixCls,
    onChange: onChange
  })) : null;
};
var _default = exports.default = PanelPresets;