import _extends from "@babel/runtime/helpers/esm/extends";
import * as React from 'react';
import Cell from "../Cell";
import TableContext from "../context/TableContext";
import { useContext } from '@rc-component/context';
import { getCellFixedInfo } from "../utils/fixUtil";
import SummaryContext from "./SummaryContext";
export default function SummaryCell(_ref) {
  var className = _ref.className,
    index = _ref.index,
    children = _ref.children,
    _ref$colSpan = _ref.colSpan,
    colSpan = _ref$colSpan === void 0 ? 1 : _ref$colSpan,
    rowSpan = _ref.rowSpan,
    align = _ref.align;
  var _useContext = useContext(TableContext, ['prefixCls', 'direction']),
    prefixCls = _useContext.prefixCls,
    direction = _useContext.direction;
  var _React$useContext = React.useContext(SummaryContext),
    scrollColumnIndex = _React$useContext.scrollColumnIndex,
    stickyOffsets = _React$useContext.stickyOffsets,
    flattenColumns = _React$useContext.flattenColumns;
  var lastIndex = index + colSpan - 1;
  var mergedColSpan = lastIndex + 1 === scrollColumnIndex ? colSpan + 1 : colSpan;
  var fixedInfo = getCellFixedInfo(index, index + mergedColSpan - 1, flattenColumns, stickyOffsets, direction);
  return /*#__PURE__*/React.createElement(Cell, _extends({
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