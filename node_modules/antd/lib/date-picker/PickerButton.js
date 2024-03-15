"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = PickerButton;
var React = _interopRequireWildcard(require("react"));
var _button = _interopRequireDefault(require("../button"));
function PickerButton(props) {
  return /*#__PURE__*/React.createElement(_button.default, Object.assign({
    size: "small",
    type: "primary"
  }, props));
}