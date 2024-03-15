"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "Col", {
  enumerable: true,
  get: function () {
    return _col.default;
  }
});
Object.defineProperty(exports, "Row", {
  enumerable: true,
  get: function () {
    return _row.default;
  }
});
exports.default = void 0;
var _col = _interopRequireDefault(require("./col"));
var _useBreakpoint = _interopRequireDefault(require("./hooks/useBreakpoint"));
var _row = _interopRequireDefault(require("./row"));
// Do not export params
function useBreakpoint() {
  return (0, _useBreakpoint.default)();
}
var _default = exports.default = {
  useBreakpoint
};