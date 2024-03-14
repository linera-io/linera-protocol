import * as React from 'react';
import { renderColumnTitle } from '../util';
function fillTitle(columns, columnTitleProps) {
  return columns.map(column => {
    const cloneColumn = Object.assign({}, column);
    cloneColumn.title = renderColumnTitle(column.title, columnTitleProps);
    if ('children' in cloneColumn) {
      cloneColumn.children = fillTitle(cloneColumn.children, columnTitleProps);
    }
    return cloneColumn;
  });
}
export default function useTitleColumns(columnTitleProps) {
  const filledColumns = React.useCallback(columns => fillTitle(columns, columnTitleProps), [columnTitleProps]);
  return [filledColumns];
}