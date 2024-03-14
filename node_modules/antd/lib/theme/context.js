"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.defaultTheme = exports.defaultConfig = exports.DesignTokenContext = void 0;
var _react = _interopRequireDefault(require("react"));
var _cssinjs = require("@ant-design/cssinjs");
var _default = _interopRequireDefault(require("./themes/default"));
var _seed = _interopRequireDefault(require("./themes/seed"));
const defaultTheme = exports.defaultTheme = (0, _cssinjs.createTheme)(_default.default);
// ================================ Context =================================
// To ensure snapshot stable. We disable hashed in test env.
const defaultConfig = exports.defaultConfig = {
  token: _seed.default,
  override: {
    override: _seed.default
  },
  hashed: true
};
const DesignTokenContext = exports.DesignTokenContext = /*#__PURE__*/_react.default.createContext(defaultConfig);