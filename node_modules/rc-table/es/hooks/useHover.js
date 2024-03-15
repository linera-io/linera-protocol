import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
export default function useHover() {
  var _React$useState = React.useState(-1),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    startRow = _React$useState2[0],
    setStartRow = _React$useState2[1];
  var _React$useState3 = React.useState(-1),
    _React$useState4 = _slicedToArray(_React$useState3, 2),
    endRow = _React$useState4[0],
    setEndRow = _React$useState4[1];
  var onHover = React.useCallback(function (start, end) {
    setStartRow(start);
    setEndRow(end);
  }, []);
  return [startRow, endRow, onHover];
}