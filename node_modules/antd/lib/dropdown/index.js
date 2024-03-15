"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _dropdown = _interopRequireDefault(require("./dropdown"));
var _dropdownButton = _interopRequireDefault(require("./dropdown-button"));
const Dropdown = _dropdown.default;
Dropdown.Button = _dropdownButton.default;
var _default = exports.default = Dropdown;