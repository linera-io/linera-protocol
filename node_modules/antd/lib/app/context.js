"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.AppConfigContext = void 0;
var _react = _interopRequireDefault(require("react"));
const AppConfigContext = exports.AppConfigContext = /*#__PURE__*/_react.default.createContext({});
const AppContext = /*#__PURE__*/_react.default.createContext({
  message: {},
  notification: {},
  modal: {}
});
var _default = exports.default = AppContext;