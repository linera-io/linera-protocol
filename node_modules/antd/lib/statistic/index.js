"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Countdown = _interopRequireDefault(require("./Countdown"));
var _Statistic = _interopRequireDefault(require("./Statistic"));
_Statistic.default.Countdown = _Countdown.default;
var _default = exports.default = _Statistic.default;