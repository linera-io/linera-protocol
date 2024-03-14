"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.INTERNAL_COL_DEFINE = void 0;
exports.getExpandableProps = getExpandableProps;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _excluded = ["expandable"];
var INTERNAL_COL_DEFINE = exports.INTERNAL_COL_DEFINE = 'RC_TABLE_INTERNAL_COL_DEFINE';
function getExpandableProps(props) {
  var expandable = props.expandable,
    legacyExpandableConfig = (0, _objectWithoutProperties2.default)(props, _excluded);
  var config;
  if ('expandable' in props) {
    config = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, legacyExpandableConfig), expandable);
  } else {
    if (process.env.NODE_ENV !== 'production' && ['indentSize', 'expandedRowKeys', 'defaultExpandedRowKeys', 'defaultExpandAllRows', 'expandedRowRender', 'expandRowByClick', 'expandIcon', 'onExpand', 'onExpandedRowsChange', 'expandedRowClassName', 'expandIconColumnIndex', 'showExpandColumn', 'title'].some(function (prop) {
      return prop in props;
    })) {
      (0, _warning.default)(false, 'expanded related props have been moved into `expandable`.');
    }
    config = legacyExpandableConfig;
  }
  if (config.showExpandColumn === false) {
    config.expandIconColumnIndex = -1;
  }
  return config;
}