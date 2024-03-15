"use client";

import * as React from 'react';
import { EXPAND_COLUMN, Summary } from 'rc-table';
import Column from './Column';
import ColumnGroup from './ColumnGroup';
import { SELECTION_ALL, SELECTION_COLUMN, SELECTION_INVERT, SELECTION_NONE } from './hooks/useSelection';
import InternalTable from './InternalTable';
const Table = (props, ref) => {
  const renderTimesRef = React.useRef(0);
  renderTimesRef.current += 1;
  return /*#__PURE__*/React.createElement(InternalTable, Object.assign({}, props, {
    ref: ref,
    _renderTimes: renderTimesRef.current
  }));
};
const ForwardTable = /*#__PURE__*/React.forwardRef(Table);
ForwardTable.SELECTION_COLUMN = SELECTION_COLUMN;
ForwardTable.EXPAND_COLUMN = EXPAND_COLUMN;
ForwardTable.SELECTION_ALL = SELECTION_ALL;
ForwardTable.SELECTION_INVERT = SELECTION_INVERT;
ForwardTable.SELECTION_NONE = SELECTION_NONE;
ForwardTable.Column = Column;
ForwardTable.ColumnGroup = ColumnGroup;
ForwardTable.Summary = Summary;
if (process.env.NODE_ENV !== 'production') {
  ForwardTable.displayName = 'Table';
}
export default ForwardTable;