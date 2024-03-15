"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
Object.defineProperty(exports, "useMutateObserver", {
  enumerable: true,
  get: function get() {
    return _useMutateObserver.default;
  }
});
var _MutateObserver = _interopRequireDefault(require("./MutateObserver"));
var _useMutateObserver = _interopRequireDefault(require("./useMutateObserver"));
var _default = _MutateObserver.default;
exports.default = _default;