"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _rcTable = require("rc-table");
var _Column = _interopRequireDefault(require("./Column"));
var _ColumnGroup = _interopRequireDefault(require("./ColumnGroup"));
var _useSelection = require("./hooks/useSelection");
var _InternalTable = _interopRequireDefault(require("./InternalTable"));
const Table = (props, ref) => {
  const renderTimesRef = React.useRef(0);
  renderTimesRef.current += 1;
  return /*#__PURE__*/React.createElement(_InternalTable.default, Object.assign({}, props, {
    ref: ref,
    _renderTimes: renderTimesRef.current
  }));
};
const ForwardTable = /*#__PURE__*/React.forwardRef(Table);
ForwardTable.SELECTION_COLUMN = _useSelection.SELECTION_COLUMN;
ForwardTable.EXPAND_COLUMN = _rcTable.EXPAND_COLUMN;
ForwardTable.SELECTION_ALL = _useSelection.SELECTION_ALL;
ForwardTable.SELECTION_INVERT = _useSelection.SELECTION_INVERT;
ForwardTable.SELECTION_NONE = _useSelection.SELECTION_NONE;
ForwardTable.Column = _Column.default;
ForwardTable.ColumnGroup = _ColumnGroup.default;
ForwardTable.Summary = _rcTable.Summary;
if (process.env.NODE_ENV !== 'production') {
  ForwardTable.displayName = 'Table';
}
var _default = exports.default = ForwardTable;