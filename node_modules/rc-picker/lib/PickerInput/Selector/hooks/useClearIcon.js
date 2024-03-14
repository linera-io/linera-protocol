"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.fillClearIcon = fillClearIcon;
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var React = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/**
 * Used for `useFilledProps` since it already in the React.useMemo
 */
function fillClearIcon(prefixCls, allowClear, clearIcon) {
  if (process.env.NODE_ENV !== 'production' && clearIcon) {
    (0, _warning.default)(false, '`clearIcon` will be removed in future. Please use `allowClear` instead.');
  }
  if (allowClear === false) {
    return null;
  }
  var config = allowClear && (0, _typeof2.default)(allowClear) === 'object' ? allowClear : {};
  return config.clearIcon || clearIcon || /*#__PURE__*/React.createElement("span", {
    className: "".concat(prefixCls, "-clear-btn")
  });
}