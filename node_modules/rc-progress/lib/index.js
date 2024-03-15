"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "Circle", {
  enumerable: true,
  get: function get() {
    return _Circle.default;
  }
});
Object.defineProperty(exports, "Line", {
  enumerable: true,
  get: function get() {
    return _Line.default;
  }
});
exports.default = void 0;
var _Line = _interopRequireDefault(require("./Line"));
var _Circle = _interopRequireDefault(require("./Circle"));
var _default = {
  Line: _Line.default,
  Circle: _Circle.default
};
exports.default = _default;