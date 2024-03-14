"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = require("react");
var _DisabledContext = _interopRequireDefault(require("../DisabledContext"));
var _SizeContext = _interopRequireDefault(require("../SizeContext"));
function useConfig() {
  const componentDisabled = (0, _react.useContext)(_DisabledContext.default);
  const componentSize = (0, _react.useContext)(_SizeContext.default);
  return {
    componentDisabled,
    componentSize
  };
}
var _default = exports.default = useConfig;