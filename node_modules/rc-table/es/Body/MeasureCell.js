import * as React from 'react';
import ResizeObserver from 'rc-resize-observer';
export default function MeasureCell(_ref) {
  var columnKey = _ref.columnKey,
    onColumnResize = _ref.onColumnResize;
  var cellRef = React.useRef();
  React.useEffect(function () {
    if (cellRef.current) {
      onColumnResize(columnKey, cellRef.current.offsetWidth);
    }
  }, []);
  return /*#__PURE__*/React.createElement(ResizeObserver, {
    data: columnKey
  }, /*#__PURE__*/React.createElement("td", {
    ref: cellRef,
    style: {
      padding: 0,
      border: 0,
      height: 0
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      height: 0,
      overflow: 'hidden'
    }
  }, "\xA0")));
}