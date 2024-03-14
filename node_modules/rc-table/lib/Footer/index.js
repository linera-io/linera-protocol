"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.FooterComponents = void 0;
var _context = require("@rc-component/context");
var React = _interopRequireWildcard(require("react"));
var _TableContext = _interopRequireWildcard(require("../context/TableContext"));
var _useRenderTimes = _interopRequireDefault(require("../hooks/useRenderTimes"));
var _Summary = _interopRequireDefault(require("./Summary"));
var _SummaryContext = _interopRequireDefault(require("./SummaryContext"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Footer(props) {
  if (process.env.NODE_ENV !== 'production') {
    (0, _useRenderTimes.default)(props);
  }
  var children = props.children,
    stickyOffsets = props.stickyOffsets,
    flattenColumns = props.flattenColumns;
  var prefixCls = (0, _context.useContext)(_TableContext.default, 'prefixCls');
  var lastColumnIndex = flattenColumns.length - 1;
  var scrollColumn = flattenColumns[lastColumnIndex];
  var summaryContext = React.useMemo(function () {
    return {
      stickyOffsets: stickyOffsets,
      flattenColumns: flattenColumns,
      scrollColumnIndex: scrollColumn !== null && scrollColumn !== void 0 && scrollColumn.scrollbar ? lastColumnIndex : null
    };
  }, [scrollColumn, flattenColumns, lastColumnIndex, stickyOffsets]);
  return /*#__PURE__*/React.createElement(_SummaryContext.default.Provider, {
    value: summaryContext
  }, /*#__PURE__*/React.createElement("tfoot", {
    className: "".concat(prefixCls, "-summary")
  }, children));
}
var _default = exports.default = (0, _TableContext.responseImmutable)(Footer);
var FooterComponents = exports.FooterComponents = _Summary.default;