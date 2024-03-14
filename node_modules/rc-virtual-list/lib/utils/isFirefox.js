"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var isFF = (typeof navigator === "undefined" ? "undefined" : (0, _typeof2.default)(navigator)) === 'object' && /Firefox/i.test(navigator.userAgent);
var _default = exports.default = isFF;