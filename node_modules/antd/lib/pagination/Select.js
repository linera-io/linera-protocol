"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.MiniSelect = exports.MiddleSelect = void 0;
var React = _interopRequireWildcard(require("react"));
var _select = _interopRequireDefault(require("../select"));
const MiniSelect = props => /*#__PURE__*/React.createElement(_select.default, Object.assign({}, props, {
  showSearch: true,
  size: "small"
}));
exports.MiniSelect = MiniSelect;
const MiddleSelect = props => /*#__PURE__*/React.createElement(_select.default, Object.assign({}, props, {
  showSearch: true,
  size: "middle"
}));
exports.MiddleSelect = MiddleSelect;
MiniSelect.Option = _select.default.Option;
MiddleSelect.Option = _select.default.Option;