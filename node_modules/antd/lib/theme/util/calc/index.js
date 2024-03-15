"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _NumCalculator = _interopRequireDefault(require("./NumCalculator"));
var _CSSCalculator = _interopRequireDefault(require("./CSSCalculator"));
const genCalc = type => {
  const Calculator = type === 'css' ? _CSSCalculator.default : _NumCalculator.default;
  return num => new Calculator(num);
};
var _default = exports.default = genCalc;