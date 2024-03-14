import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import { useContext } from '@rc-component/context';
import classNames from 'classnames';
import * as React from 'react';
import { getCellProps } from "../Body/BodyRow";
import Cell from "../Cell";
import { GridContext } from "./context";
/**
 * Return the width of the column by `colSpan`.
 * When `colSpan` is `0` will be trade as `1`.
 */
export function getColumnWidth(colIndex, colSpan, columnsOffset) {
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
  var _useContext = useContext(GridContext, ['columnsOffset']),
    columnsOffset = _useContext.columnsOffset;
  var _getCellProps = getCellProps(rowInfo, column, colIndex, indent, index),
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
  var mergedStyle = _objectSpread(_objectSpread(_objectSpread({}, cellStyle), style), {}, {
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
  return /*#__PURE__*/React.createElement(Cell, _extends({
    className: classNames(columnClassName, className),
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
    additionalProps: _objectSpread(_objectSpread({}, additionalCellProps), {}, {
      style: mergedStyle
    }, cellSpan)
  }));
}
export default VirtualCell;