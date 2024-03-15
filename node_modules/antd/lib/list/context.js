"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ListContext = exports.ListConsumer = void 0;
var _react = _interopRequireDefault(require("react"));
const ListContext = exports.ListContext = /*#__PURE__*/_react.default.createContext({});
const ListConsumer = exports.ListConsumer = ListContext.Consumer;