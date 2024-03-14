"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
function useItems(items, children) {
  if (items && Array.isArray(items)) {
    return items;
  }
  return (0, _toArray.default)(children).map(ele => {
    var _a, _b;
    return Object.assign({
      children: (_b = (_a = ele === null || ele === void 0 ? void 0 : ele.props) === null || _a === void 0 ? void 0 : _a.children) !== null && _b !== void 0 ? _b : ''
    }, ele.props);
  });
}
var _default = exports.default = useItems;