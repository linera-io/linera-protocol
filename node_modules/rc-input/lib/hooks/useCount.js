"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useCount;
exports.inCountRange = inCountRange;
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var React = _interopRequireWildcard(require("react"));
var _excluded = ["show"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/**
 * Cut `value` by the `count.max` prop.
 */
function inCountRange(value, countConfig) {
  if (!countConfig.max) {
    return true;
  }
  var count = countConfig.strategy(value);
  return count <= countConfig.max;
}
function useCount(count, showCount) {
  return React.useMemo(function () {
    var mergedConfig = {};
    if (showCount) {
      mergedConfig.show = (0, _typeof2.default)(showCount) === 'object' && showCount.formatter ? showCount.formatter : !!showCount;
    }
    mergedConfig = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, mergedConfig), count);
    var _ref = mergedConfig,
      show = _ref.show,
      rest = (0, _objectWithoutProperties2.default)(_ref, _excluded);
    return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, rest), {}, {
      show: !!show,
      showFormatter: typeof show === 'function' ? show : undefined,
      strategy: rest.strategy || function (value) {
        return value.length;
      }
    });
  }, [count, showCount]);
}