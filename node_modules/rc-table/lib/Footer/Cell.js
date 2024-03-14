"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = SummaryCell;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var React = _interopRequireWildcard(require("react"));
var _Cell = _interopRequireDefault(require("../Cell"));
var _TableContext = _interopRequireDefault(require("../context/TableContext"));
var _context = require("@rc-component/context");
var _fixUtil = require("../utils/fixUtil");
var _SummaryContext = _interopRequireDefault(require("./SummaryContext"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function SummaryCell(_ref) {
  var className = _ref.className,
    index = _ref.index,
    children = _ref.children,
    _ref$colSpan = _ref.colSpan,
    colSpan = _ref$colSpan === void 0 ? 1 : _ref$colSpan,
    rowSpan = _ref.rowSpan,
    align = _ref.align;
  var _useContext = (0, _context.useContext)(_TableContext.default, ['prefixCls', 'direction']),
    prefixCls = _useContext.prefixCls,
    direction = _useContext.direction;
  var _React$useContext = React.useContext(_SummaryContext.default),
    scrollColumnIndex = _React$useContext.scrollColumnIndex,
    stickyOffsets = _React$useContext.stickyOffsets,
    flattenColumns = _React$useContext.flattenColumns;
  var lastIndex = index + colSpan - 1;
  var mergedColSpan = lastIndex + 1 === scrollColumnIndex ? colSpan + 1 : colSpan;
  var fixedInfo = (0, _fixUtil.getCellFixedInfo)(index, index + mergedColSpan - 1, flattenColumns, stickyOffsets, direction);
  return /*#__PURE__*/React.createElement(_Cell.default, (0, _extends2.default)({
    className: className,
    index: index,
    component: "td",
    prefixCls: prefixCls,
    record: null,
    dataIndex: null,
    align: align,
    colSpan: mergedColSpan,
    rowSpan: rowSpan,
    render: function render() {
      return children;
    }
  }, fixedInfo));
}