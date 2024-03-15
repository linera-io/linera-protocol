import { useContext } from '@rc-component/context';
import * as React from 'react';
import Cell from "../Cell";
import TableContext from "../context/TableContext";
import devRenderTimes from "../hooks/useRenderTimes";
function ExpandedRow(props) {
  if (process.env.NODE_ENV !== 'production') {
    devRenderTimes(props);
  }
  var prefixCls = props.prefixCls,
    children = props.children,
    Component = props.component,
    cellComponent = props.cellComponent,
    className = props.className,
    expanded = props.expanded,
    colSpan = props.colSpan,
    isEmpty = props.isEmpty;
  var _useContext = useContext(TableContext, ['scrollbarSize', 'fixHeader', 'fixColumn', 'componentWidth', 'horizonScroll']),
    scrollbarSize = _useContext.scrollbarSize,
    fixHeader = _useContext.fixHeader,
    fixColumn = _useContext.fixColumn,
    componentWidth = _useContext.componentWidth,
    horizonScroll = _useContext.horizonScroll;

  // Cache render node
  var contentNode = children;
  if (isEmpty ? horizonScroll && componentWidth : fixColumn) {
    contentNode = /*#__PURE__*/React.createElement("div", {
      style: {
        width: componentWidth - (fixHeader ? scrollbarSize : 0),
        position: 'sticky',
        left: 0,
        overflow: 'hidden'
      },
      className: "".concat(prefixCls, "-expanded-row-fixed")
    }, contentNode);
  }
  return /*#__PURE__*/React.createElement(Component, {
    className: className,
    style: {
      display: expanded ? null : 'none'
    }
  }, /*#__PURE__*/React.createElement(Cell, {
    component: cellComponent,
    prefixCls: prefixCls,
    colSpan: colSpan
  }, contentNode));
}
export default ExpandedRow;