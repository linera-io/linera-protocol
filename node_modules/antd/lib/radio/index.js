"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "Button", {
  enumerable: true,
  get: function () {
    return _radioButton.default;
  }
});
Object.defineProperty(exports, "Group", {
  enumerable: true,
  get: function () {
    return _group.default;
  }
});
exports.default = void 0;
var _group = _interopRequireDefault(require("./group"));
var _radio = _interopRequireDefault(require("./radio"));
var _radioButton = _interopRequireDefault(require("./radioButton"));
const Radio = _radio.default;
Radio.Button = _radioButton.default;
Radio.Group = _group.default;
Radio.__ANT_RADIO = true;
var _default = exports.default = Radio;