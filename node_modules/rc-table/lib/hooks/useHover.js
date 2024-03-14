"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useHover;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function useHover() {
  var _React$useState = React.useState(-1),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    startRow = _React$useState2[0],
    setStartRow = _React$useState2[1];
  var _React$useState3 = React.useState(-1),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    endRow = _React$useState4[0],
    setEndRow = _React$useState4[1];
  var onHover = React.useCallback(function (start, end) {
    setStartRow(start);
    setEndRow(end);
  }, []);
  return [startRow, endRow, onHover];
}