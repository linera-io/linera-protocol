import { useContext } from '@rc-component/context';
import * as React from 'react';
import PerfContext from "../context/PerfContext";
import TableContext, { responseImmutable } from "../context/TableContext";
import useFlattenRecords from "../hooks/useFlattenRecords";
import devRenderTimes from "../hooks/useRenderTimes";
import { getColumnsKey } from "../utils/valueUtil";
import BodyRow from "./BodyRow";
import ExpandedRow from "./ExpandedRow";
import MeasureRow from "./MeasureRow";
function Body(props) {
  if (process.env.NODE_ENV !== 'production') {
    devRenderTimes(props);
  }
  var data = props.data,
    measureColumnWidth = props.measureColumnWidth;
  var _useContext = useContext(TableContext, ['prefixCls', 'getComponent', 'onColumnResize', 'flattenColumns', 'getRowKey', 'expandedKeys', 'childrenColumnName', 'emptyNode']),
    prefixCls = _useContext.prefixCls,
    getComponent = _useContext.getComponent,
    onColumnResize = _useContext.onColumnResize,
    flattenColumns = _useContext.flattenColumns,
    getRowKey = _useContext.getRowKey,
    expandedKeys = _useContext.expandedKeys,
    childrenColumnName = _useContext.childrenColumnName,
    emptyNode = _useContext.emptyNode;
  var flattenData = useFlattenRecords(data, childrenColumnName, expandedKeys, getRowKey);

  // =================== Performance ====================
  var perfRef = React.useRef({
    renderWithProps: false
  });

  // ====================== Render ======================
  var WrapperComponent = getComponent(['body', 'wrapper'], 'tbody');
  var trComponent = getComponent(['body', 'row'], 'tr');
  var tdComponent = getComponent(['body', 'cell'], 'td');
  var thComponent = getComponent(['body', 'cell'], 'th');
  var rows;
  if (data.length) {
    rows = flattenData.map(function (item, idx) {
      var record = item.record,
        indent = item.indent,
        renderIndex = item.index;
      var key = getRowKey(record, idx);
      return /*#__PURE__*/React.createElement(BodyRow, {
        key: key,
        rowKey: key,
        record: record,
        index: idx,
        renderIndex: renderIndex,
        rowComponent: trComponent,
        cellComponent: tdComponent,
        scopeCellComponent: thComponent,
        getRowKey: getRowKey,
        indent: indent
      });
    });
  } else {
    rows = /*#__PURE__*/React.createElement(ExpandedRow, {
      expanded: true,
      className: "".concat(prefixCls, "-placeholder"),
      prefixCls: prefixCls,
      component: trComponent,
      cellComponent: tdComponent,
      colSpan: flattenColumns.length,
      isEmpty: true
    }, emptyNode);
  }
  var columnsKey = getColumnsKey(flattenColumns);
  return /*#__PURE__*/React.createElement(PerfContext.Provider, {
    value: perfRef.current
  }, /*#__PURE__*/React.createElement(WrapperComponent, {
    className: "".concat(prefixCls, "-tbody")
  }, measureColumnWidth && /*#__PURE__*/React.createElement(MeasureRow, {
    prefixCls: prefixCls,
    columnsKey: columnsKey,
    onColumnResize: onColumnResize
  }), rows));
}
if (process.env.NODE_ENV !== 'production') {
  Body.displayName = 'Body';
}
export default responseImmutable(Body);