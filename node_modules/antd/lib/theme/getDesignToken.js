"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _default2 = _interopRequireDefault(require("./themes/default"));
var _seed = _interopRequireDefault(require("./themes/seed"));
var _alias = _interopRequireDefault(require("./util/alias"));
const getDesignToken = config => {
  const theme = (config === null || config === void 0 ? void 0 : config.algorithm) ? (0, _cssinjs.createTheme)(config.algorithm) : (0, _cssinjs.createTheme)(_default2.default);
  const mergedToken = Object.assign(Object.assign({}, _seed.default), config === null || config === void 0 ? void 0 : config.token);
  return (0, _cssinjs.getComputedToken)(mergedToken, {
    override: config === null || config === void 0 ? void 0 : config.token
  }, theme, _alias.default);
};
var _default = exports.default = getDesignToken;