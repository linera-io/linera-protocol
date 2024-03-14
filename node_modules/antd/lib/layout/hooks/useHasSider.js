"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useHasSider;
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _Sider = _interopRequireDefault(require("../Sider"));
function useHasSider(siders, children, hasSider) {
  if (typeof hasSider === 'boolean') {
    return hasSider;
  }
  if (siders.length) {
    return true;
  }
  const childNodes = (0, _toArray.default)(children);
  return childNodes.some(node => node.type === _Sider.default);
}