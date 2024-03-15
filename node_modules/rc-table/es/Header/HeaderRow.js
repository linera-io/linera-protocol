import _extends from "@babel/runtime/helpers/esm/extends";
import * as React from 'react';
import Cell from "../Cell";
import TableContext from "../context/TableContext";
import { useContext } from '@rc-component/context';
import { getCellFixedInfo } from "../utils/fixUtil";
import { getColumnsKey } from "../utils/valueUtil";
var HeaderRow = function HeaderRow(props) {
  var cells = props.cells,
    stickyOffsets = props.stickyOffsets,
    flattenColumns = props.flattenColumns,
    RowComponent = props.rowComponent,
    CellComponent = props.cellComponent,
    onHeaderRow = props.onHeaderRow,
    index = props.index;
  var _useContext = useContext(TableContext, ['prefixCls', 'direction']),
    prefixCls = _useContext.prefixCls,
    direction = _useContext.direction;
  var rowProps;
  if (onHeaderRow) {
    rowProps = onHeaderRow(cells.map(function (cell) {
      return cell.column;
    }), index);
  }
  var columnsKey = getColumnsKey(cells.map(function (cell) {
    return cell.column;
  }));
  return /*#__PURE__*/React.createElement(RowComponent, rowProps, cells.map(function (cell, cellIndex) {
    var column = cell.column;
    var fixedInfo = getCellFixedInfo(cell.colStart, cell.colEnd, flattenColumns, stickyOffsets, direction);
    var additionalProps;
    if (column && column.onHeaderCell) {
      additionalProps = cell.column.onHeaderCell(column);
    }
    return /*#__PURE__*/React.createElement(Cell, _extends({}, cell, {
      scope: column.title ? cell.colSpan > 1 ? 'colgroup' : 'col' : null,
      ellipsis: column.ellipsis,
      align: column.align,
      component: CellComponent,
      prefixCls: prefixCls,
      key: columnsKey[cellIndex]
    }, fixedInfo, {
      additionalProps: additionalProps,
      rowType: "header"
    }));
  }));
};
if (process.env.NODE_ENV !== 'production') {
  HeaderRow.displayName = 'HeaderRow';
}
export default HeaderRow;