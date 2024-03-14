import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["columnType"];
import * as React from 'react';
import { INTERNAL_COL_DEFINE } from "./utils/legacyUtil";
function ColGroup(_ref) {
  var colWidths = _ref.colWidths,
    columns = _ref.columns,
    columCount = _ref.columCount;
  var cols = [];
  var len = columCount || columns.length;

  // Only insert col with width & additional props
  // Skip if rest col do not have any useful info
  var mustInsert = false;
  for (var i = len - 1; i >= 0; i -= 1) {
    var width = colWidths[i];
    var column = columns && columns[i];
    var additionalProps = column && column[INTERNAL_COL_DEFINE];
    if (width || additionalProps || mustInsert) {
      var _ref2 = additionalProps || {},
        columnType = _ref2.columnType,
        restAdditionalProps = _objectWithoutProperties(_ref2, _excluded);
      cols.unshift( /*#__PURE__*/React.createElement("col", _extends({
        key: i,
        style: {
          width: width
        }
      }, restAdditionalProps)));
      mustInsert = true;
    }
  }
  return /*#__PURE__*/React.createElement("colgroup", null, cols);
}
export default ColGroup;