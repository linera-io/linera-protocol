"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useTitleColumns;
var React = _interopRequireWildcard(require("react"));
var _util = require("../util");
function fillTitle(columns, columnTitleProps) {
  return columns.map(column => {
    const cloneColumn = Object.assign({}, column);
    cloneColumn.title = (0, _util.renderColumnTitle)(column.title, columnTitleProps);
    if ('children' in cloneColumn) {
      cloneColumn.children = fillTitle(cloneColumn.children, columnTitleProps);
    }
    return cloneColumn;
  });
}
function useTitleColumns(columnTitleProps) {
  const filledColumns = React.useCallback(columns => fillTitle(columns, columnTitleProps), [columnTitleProps]);
  return [filledColumns];
}