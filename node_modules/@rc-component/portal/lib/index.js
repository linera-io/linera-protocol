"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
Object.defineProperty(exports, "inlineMock", {
  enumerable: true,
  get: function get() {
    return _mock.inlineMock;
  }
});
var _Portal = _interopRequireDefault(require("./Portal"));
var _mock = require("./mock");
var _default = _Portal.default;
exports.default = _default;