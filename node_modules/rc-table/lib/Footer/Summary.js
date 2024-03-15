"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Cell = _interopRequireDefault(require("./Cell"));
var _Row = _interopRequireDefault(require("./Row"));
/**
 * Syntactic sugar. Do not support HOC.
 */
function Summary(_ref) {
  var children = _ref.children;
  return children;
}
Summary.Row = _Row.default;
Summary.Cell = _Cell.default;
var _default = exports.default = Summary;