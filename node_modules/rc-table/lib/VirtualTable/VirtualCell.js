"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
exports.getColumnWidth = getColumnWidth;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _context = require("@rc-component/context");
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _BodyRow = require("../Body/BodyRow");
var _Cell = _interopRequireDefault(require("../Cell"));
var _context2 = require("./context");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/**
 * Return the width of the column by `colSpan`.
 * When `colSpan` is `0` will be trade as `1`.
 */
function getColumnWidth(colIndex, colSpan, columnsOffset) {
  var mergedColSpan = colSpan || 1;
  return columnsOffset[colIndex + mergedColSpan] - (columnsOffset[colIndex] || 0);
}
function VirtualCell(props) {
  var rowInfo = props.rowInfo,
    column = props.column,
    colIndex = props.colIndex,
    indent = props.indent,
    index = props.index,
    component = props.component,
    renderIndex = props.renderIndex,
    record = props.record,
    style = props.style,
    className = props.className,
    inverse = props.inverse,
    getHeight = props.getHeight;
  var render = column.render,
    dataIndex = column.dataIndex,
    columnClassName = column.className,
    colWidth = column.width;
  var _useContext = (0, _context.useContext)(_context2.GridContext, ['columnsOffset']),
    columnsOffset = _useContext.columnsOffset;
  var _getCellProps = (0, _BodyRow.getCellProps)(rowInfo, column, colIndex, indent, index),
    key = _getCellProps.key,
    fixedInfo = _getCellProps.fixedInfo,
    appendCellNode = _getCellProps.appendCellNode,
    additionalCellProps = _getCellProps.additionalCellProps;
  var cellStyle = additionalCellProps.style,
    _additionalCellProps$ = additionalCellProps.colSpan,
    colSpan = _additionalCellProps$ === void 0 ? 1 : _additionalCellProps$,
    _additionalCellProps$2 = additionalCellProps.rowSpan,
    rowSpan = _additionalCellProps$2 === void 0 ? 1 : _additionalCellProps$2;

  // ========================= ColWidth =========================
  // column width
  var startColIndex = colIndex - 1;
  var concatColWidth = getColumnWidth(startColIndex, colSpan, columnsOffset);

  // margin offset
  var marginOffset = colSpan > 1 ? colWidth - concatColWidth : 0;

  // ========================== Style ===========================
  var mergedStyle = (0, _objectSpread2.default)((0, _objectSpread2.default)((0, _objectSpread2.default)({}, cellStyle), style), {}, {
    flex: "0 0 ".concat(concatColWidth, "px"),
    width: "".concat(concatColWidth, "px"),
    marginRight: marginOffset,
    pointerEvents: 'auto'
  });

  // When `colSpan` or `rowSpan` is `0`, should skip render.
  var needHide = React.useMemo(function () {
    if (inverse) {
      return rowSpan <= 1;
    } else {
      return colSpan === 0 || rowSpan === 0 || rowSpan > 1;
    }
  }, [rowSpan, colSpan, inverse]);

  // 0 rowSpan or colSpan should not render
  if (needHide) {
    mergedStyle.visibility = 'hidden';
  } else if (inverse) {
    mergedStyle.height = getHeight === null || getHeight === void 0 ? void 0 : getHeight(rowSpan);
  }
  var mergedRender = needHide ? function () {
    return null;
  } : render;

  // ========================== Render ==========================
  var cellSpan = {};

  // Virtual should reset `colSpan` & `rowSpan`
  if (rowSpan === 0 || colSpan === 0) {
    cellSpan.rowSpan = 1;
    cellSpan.colSpan = 1;
  }
  return /*#__PURE__*/React.createElement(_Cell.default, (0, _extends2.default)({
    className: (0, _classnames.default)(columnClassName, className),
    ellipsis: column.ellipsis,
    align: column.align,
    scope: column.rowScope,
    component: component,
    prefixCls: rowInfo.prefixCls,
    key: key,
    record: record,
    index: index,
    renderIndex: renderIndex,
    dataIndex: dataIndex,
    render: mergedRender,
    shouldCellUpdate: column.shouldCellUpdate
  }, fixedInfo, {
    appendNode: appendCellNode,
    additionalProps: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, additionalCellProps), {}, {
      style: mergedStyle
    }, cellSpan)
  }));
}
var _default = exports.default = VirtualCell;