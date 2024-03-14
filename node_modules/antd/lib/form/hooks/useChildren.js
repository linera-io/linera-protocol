"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useChildren;
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
function useChildren(children) {
  if (typeof children === 'function') {
    return children;
  }
  const childList = (0, _toArray.default)(children);
  return childList.length <= 1 ? childList[0] : childList;
}