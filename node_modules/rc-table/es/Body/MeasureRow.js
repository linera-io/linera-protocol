import * as React from 'react';
import ResizeObserver from 'rc-resize-observer';
import MeasureCell from "./MeasureCell";
export default function MeasureRow(_ref) {
  var prefixCls = _ref.prefixCls,
    columnsKey = _ref.columnsKey,
    onColumnResize = _ref.onColumnResize;
  return /*#__PURE__*/React.createElement("tr", {
    "aria-hidden": "true",
    className: "".concat(prefixCls, "-measure-row"),
    style: {
      height: 0,
      fontSize: 0
    }
  }, /*#__PURE__*/React.createElement(ResizeObserver.Collection, {
    onBatchResize: function onBatchResize(infoList) {
      infoList.forEach(function (_ref2) {
        var columnKey = _ref2.data,
          size = _ref2.size;
        onColumnResize(columnKey, size.offsetWidth);
      });
    }
  }, columnsKey.map(function (columnKey) {
    return /*#__PURE__*/React.createElement(MeasureCell, {
      key: columnKey,
      columnKey: columnKey,
      onColumnResize: onColumnResize
    });
  })));
}