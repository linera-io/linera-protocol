"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Alert = _interopRequireDefault(require("./Alert"));
var _ErrorBoundary = _interopRequireDefault(require("./ErrorBoundary"));
const Alert = _Alert.default;
Alert.ErrorBoundary = _ErrorBoundary.default;
var _default = exports.default = Alert;