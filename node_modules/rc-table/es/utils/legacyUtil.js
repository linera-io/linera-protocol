import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["expandable"];
import warning from "rc-util/es/warning";
export var INTERNAL_COL_DEFINE = 'RC_TABLE_INTERNAL_COL_DEFINE';
export function getExpandableProps(props) {
  var expandable = props.expandable,
    legacyExpandableConfig = _objectWithoutProperties(props, _excluded);
  var config;
  if ('expandable' in props) {
    config = _objectSpread(_objectSpread({}, legacyExpandableConfig), expandable);
  } else {
    if (process.env.NODE_ENV !== 'production' && ['indentSize', 'expandedRowKeys', 'defaultExpandedRowKeys', 'defaultExpandAllRows', 'expandedRowRender', 'expandRowByClick', 'expandIcon', 'onExpand', 'onExpandedRowsChange', 'expandedRowClassName', 'expandIconColumnIndex', 'showExpandColumn', 'title'].some(function (prop) {
      return prop in props;
    })) {
      warning(false, 'expanded related props have been moved into `expandable`.');
    }
    config = legacyExpandableConfig;
  }
  if (config.showExpandColumn === false) {
    config.expandIconColumnIndex = -1;
  }
  return config;
}