"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _context = require("@rc-component/context");
var React = _interopRequireWildcard(require("react"));
var _TableContext = _interopRequireWildcard(require("../context/TableContext"));
var _useRenderTimes = _interopRequireDefault(require("../hooks/useRenderTimes"));
var _HeaderRow = _interopRequireDefault(require("./HeaderRow"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function parseHeaderRows(rootColumns) {
  var rows = [];
  function fillRowCells(columns, colIndex) {
    var rowIndex = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 0;
    // Init rows
    rows[rowIndex] = rows[rowIndex] || [];
    var currentColIndex = colIndex;
    var colSpans = columns.filter(Boolean).map(function (column) {
      var cell = {
        key: column.key,
        className: column.className || '',
        children: column.title,
        column: column,
        colStart: currentColIndex
      };
      var colSpan = 1;
      var subColumns = column.children;
      if (subColumns && subColumns.length > 0) {
        colSpan = fillRowCells(subColumns, currentColIndex, rowIndex + 1).reduce(function (total, count) {
          return total + count;
        }, 0);
        cell.hasSubColumns = true;
      }
      if ('colSpan' in column) {
        colSpan = column.colSpan;
      }
      if ('rowSpan' in column) {
        cell.rowSpan = column.rowSpan;
      }
      cell.colSpan = colSpan;
      cell.colEnd = cell.colStart + colSpan - 1;
      rows[rowIndex].push(cell);
      currentColIndex += colSpan;
      return colSpan;
    });
    return colSpans;
  }

  // Generate `rows` cell data
  fillRowCells(rootColumns, 0);

  // Handle `rowSpan`
  var rowCount = rows.length;
  var _loop = function _loop(rowIndex) {
    rows[rowIndex].forEach(function (cell) {
      if (!('rowSpan' in cell) && !cell.hasSubColumns) {
        // eslint-disable-next-line no-param-reassign
        cell.rowSpan = rowCount - rowIndex;
      }
    });
  };
  for (var rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    _loop(rowIndex);
  }
  return rows;
}
var Header = function Header(props) {
  if (process.env.NODE_ENV !== 'production') {
    (0, _useRenderTimes.default)(props);
  }
  var stickyOffsets = props.stickyOffsets,
    columns = props.columns,
    flattenColumns = props.flattenColumns,
    onHeaderRow = props.onHeaderRow;
  var _useContext = (0, _context.useContext)(_TableContext.default, ['prefixCls', 'getComponent']),
    prefixCls = _useContext.prefixCls,
    getComponent = _useContext.getComponent;
  var rows = React.useMemo(function () {
    return parseHeaderRows(columns);
  }, [columns]);
  var WrapperComponent = getComponent(['header', 'wrapper'], 'thead');
  var trComponent = getComponent(['header', 'row'], 'tr');
  var thComponent = getComponent(['header', 'cell'], 'th');
  return /*#__PURE__*/React.createElement(WrapperComponent, {
    className: "".concat(prefixCls, "-thead")
  }, rows.map(function (row, rowIndex) {
    var rowNode = /*#__PURE__*/React.createElement(_HeaderRow.default, {
      key: rowIndex,
      flattenColumns: flattenColumns,
      cells: row,
      stickyOffsets: stickyOffsets,
      rowComponent: trComponent,
      cellComponent: thComponent,
      onHeaderRow: onHeaderRow,
      index: rowIndex
    });
    return rowNode;
  }));
};
var _default = exports.default = (0, _TableContext.responseImmutable)(Header);