"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
const zIndexContext = /*#__PURE__*/_react.default.createContext(undefined);
if (process.env.NODE_ENV !== 'production') {
  zIndexContext.displayName = 'zIndexContext';
}
var _default = exports.default = zIndexContext;