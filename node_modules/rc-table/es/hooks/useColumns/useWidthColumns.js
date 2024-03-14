import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
function parseColWidth(totalWidth) {
  var width = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';
  if (typeof width === 'number') {
    return width;
  }
  if (width.endsWith('%')) {
    return totalWidth * parseFloat(width) / 100;
  }
  return null;
}

/**
 * Fill all column with width
 */
export default function useWidthColumns(flattenColumns, scrollWidth, clientWidth) {
  return React.useMemo(function () {
    // Fill width if needed
    if (scrollWidth && scrollWidth > 0) {
      var totalWidth = 0;
      var missWidthCount = 0;

      // collect not given width column
      flattenColumns.forEach(function (col) {
        var colWidth = parseColWidth(scrollWidth, col.width);
        if (colWidth) {
          totalWidth += colWidth;
        } else {
          missWidthCount += 1;
        }
      });

      // Fill width
      var maxFitWidth = Math.max(scrollWidth, clientWidth);
      var restWidth = Math.max(maxFitWidth - totalWidth, missWidthCount);
      var restCount = missWidthCount;
      var avgWidth = restWidth / missWidthCount;
      var realTotal = 0;
      var filledColumns = flattenColumns.map(function (col) {
        var clone = _objectSpread({}, col);
        var colWidth = parseColWidth(scrollWidth, clone.width);
        if (colWidth) {
          clone.width = colWidth;
        } else {
          var colAvgWidth = Math.floor(avgWidth);
          clone.width = restCount === 1 ? restWidth : colAvgWidth;
          restWidth -= colAvgWidth;
          restCount -= 1;
        }
        realTotal += clone.width;
        return clone;
      });

      // If realTotal is less than clientWidth,
      // We need extend column width
      if (realTotal < maxFitWidth) {
        var scale = maxFitWidth / realTotal;
        restWidth = maxFitWidth;
        filledColumns.forEach(function (col, index) {
          var colWidth = Math.floor(col.width * scale);
          col.width = index === filledColumns.length - 1 ? restWidth : colWidth;
          restWidth -= colWidth;
        });
      }
      return [filledColumns, Math.max(realTotal, maxFitWidth)];
    }
    return [flattenColumns, scrollWidth];
  }, [flattenColumns, scrollWidth, clientWidth]);
}