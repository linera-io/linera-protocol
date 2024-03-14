"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ModalContextProvider = exports.ModalContext = void 0;
var _react = _interopRequireDefault(require("react"));
const ModalContext = exports.ModalContext = /*#__PURE__*/_react.default.createContext({});
const {
  Provider: ModalContextProvider
} = ModalContext;
exports.ModalContextProvider = ModalContextProvider;