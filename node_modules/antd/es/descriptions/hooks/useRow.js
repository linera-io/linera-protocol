import { useMemo } from 'react';
import { devUseWarning } from '../../_util/warning';
function getFilledItem(rowItem, rowRestCol, span) {
  let clone = rowItem;
  let exceed = false;
  if (span === undefined || span > rowRestCol) {
    clone = Object.assign(Object.assign({}, rowItem), {
      span: rowRestCol
    });
    exceed = span !== undefined;
  }
  return [clone, exceed];
}
// Calculate the sum of span in a row
function getCalcRows(rowItems, mergedColumn) {
  const rows = [];
  let tmpRow = [];
  let rowRestCol = mergedColumn;
  let exceed = false;
  rowItems.filter(n => n).forEach((rowItem, index) => {
    const span = rowItem === null || rowItem === void 0 ? void 0 : rowItem.span;
    const mergedSpan = span || 1;
    // Additional handle last one
    if (index === rowItems.length - 1) {
      const [item, itemExceed] = getFilledItem(rowItem, rowRestCol, span);
      exceed = exceed || itemExceed;
      tmpRow.push(item);
      rows.push(tmpRow);
      return;
    }
    if (mergedSpan < rowRestCol) {
      rowRestCol -= mergedSpan;
      tmpRow.push(rowItem);
    } else {
      const [item, itemExceed] = getFilledItem(rowItem, rowRestCol, mergedSpan);
      exceed = exceed || itemExceed;
      tmpRow.push(item);
      rows.push(tmpRow);
      rowRestCol = mergedColumn;
      tmpRow = [];
    }
  });
  return [rows, exceed];
}
const useRow = (mergedColumn, items) => {
  const [rows, exceed] = useMemo(() => getCalcRows(items, mergedColumn), [items, mergedColumn]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Descriptions');
    process.env.NODE_ENV !== "production" ? warning(!exceed, 'usage', 'Sum of column `span` in a line not match `column` of Descriptions.') : void 0;
  }
  return rows;
};
export default useRow;