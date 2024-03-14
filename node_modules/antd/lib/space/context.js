"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.SpaceContextProvider = exports.SpaceContext = void 0;
var _react = _interopRequireDefault(require("react"));
const SpaceContext = exports.SpaceContext = /*#__PURE__*/_react.default.createContext({
  latestIndex: 0
});
const SpaceContextProvider = exports.SpaceContextProvider = SpaceContext.Provider;