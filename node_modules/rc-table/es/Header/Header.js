import { useContext } from '@rc-component/context';
import * as React from 'react';
import TableContext, { responseImmutable } from "../context/TableContext";
import devRenderTimes from "../hooks/useRenderTimes";
import HeaderRow from "./HeaderRow";
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
    devRenderTimes(props);
  }
  var stickyOffsets = props.stickyOffsets,
    columns = props.columns,
    flattenColumns = props.flattenColumns,
    onHeaderRow = props.onHeaderRow;
  var _useContext = useContext(TableContext, ['prefixCls', 'getComponent']),
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
    var rowNode = /*#__PURE__*/React.createElement(HeaderRow, {
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
export default responseImmutable(Header);