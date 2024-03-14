"use strict";

var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useLayoutEffect;
var React = _interopRequireWildcard(require("react"));
var _commonUtil = require("../utils/commonUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/* eslint-disable react-hooks/rules-of-hooks */

/**
 * Wrap `React.useLayoutEffect` which will not throw warning message in test env
 */
function useLayoutEffect(effect, deps) {
  // Never happen in test env
  if (_commonUtil.isBrowserClient) {
    /* istanbul ignore next */
    React.useLayoutEffect(effect, deps);
  } else {
    React.useEffect(effect, deps);
  }
}
/* eslint-enable */