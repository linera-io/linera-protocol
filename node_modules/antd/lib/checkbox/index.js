"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Checkbox = _interopRequireDefault(require("./Checkbox"));
var _Group = _interopRequireDefault(require("./Group"));
const Checkbox = _Checkbox.default;
Checkbox.Group = _Group.default;
Checkbox.__ANT_CHECKBOX = true;
if (process.env.NODE_ENV !== 'production') {
  Checkbox.displayName = 'Checkbox';
}
var _default = exports.default = Checkbox;