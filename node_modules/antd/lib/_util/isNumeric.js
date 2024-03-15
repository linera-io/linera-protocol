"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
const isNumeric = value => !isNaN(parseFloat(value)) && isFinite(value);
var _default = exports.default = isNumeric;