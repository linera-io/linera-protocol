import { useMemo } from 'react';
/**
 * Get sticky column offset width
 */
function useStickyOffsets(colWidths, flattenColumns, direction) {
  var stickyOffsets = useMemo(function () {
    var columnCount = flattenColumns.length;
    var getOffsets = function getOffsets(startIndex, endIndex, offset) {
      var offsets = [];
      var total = 0;
      for (var i = startIndex; i !== endIndex; i += offset) {
        offsets.push(total);
        if (flattenColumns[i].fixed) {
          total += colWidths[i] || 0;
        }
      }
      return offsets;
    };
    var startOffsets = getOffsets(0, columnCount, 1);
    var endOffsets = getOffsets(columnCount - 1, -1, -1).reverse();
    return direction === 'rtl' ? {
      left: endOffsets,
      right: startOffsets
    } : {
      left: startOffsets,
      right: endOffsets
    };
  }, [colWidths, flattenColumns, direction]);
  return stickyOffsets;
}
export default useStickyOffsets;