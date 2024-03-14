"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useRowInfo;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _context = require("@rc-component/context");
var _TableContext = _interopRequireDefault(require("../context/TableContext"));
var _valueUtil = require("../utils/valueUtil");
var _rcUtil = require("rc-util");
var _classnames = _interopRequireDefault(require("classnames"));
function useRowInfo(record, rowKey, recordIndex, indent) {
  var context = (0, _context.useContext)(_TableContext.default, ['prefixCls', 'fixedInfoList', 'flattenColumns', 'expandableType', 'expandRowByClick', 'onTriggerExpand', 'rowClassName', 'expandedRowClassName', 'indentSize', 'expandIcon', 'expandedRowRender', 'expandIconColumnIndex', 'expandedKeys', 'childrenColumnName', 'rowExpandable', 'onRow']);
  var flattenColumns = context.flattenColumns,
    expandableType = context.expandableType,
    expandedKeys = context.expandedKeys,
    childrenColumnName = context.childrenColumnName,
    onTriggerExpand = context.onTriggerExpand,
    rowExpandable = context.rowExpandable,
    onRow = context.onRow,
    expandRowByClick = context.expandRowByClick,
    rowClassName = context.rowClassName;

  // ======================= Expandable =======================
  // Only when row is not expandable and `children` exist in record
  var nestExpandable = expandableType === 'nest';
  var rowSupportExpand = expandableType === 'row' && (!rowExpandable || rowExpandable(record));
  var mergedExpandable = rowSupportExpand || nestExpandable;
  var expanded = expandedKeys && expandedKeys.has(rowKey);
  var hasNestChildren = childrenColumnName && record && record[childrenColumnName];
  var onInternalTriggerExpand = (0, _rcUtil.useEvent)(onTriggerExpand);

  // ========================= onRow ==========================
  var rowProps = onRow === null || onRow === void 0 ? void 0 : onRow(record, recordIndex);
  var onRowClick = rowProps === null || rowProps === void 0 ? void 0 : rowProps.onClick;
  var onClick = function onClick(event) {
    if (expandRowByClick && mergedExpandable) {
      onTriggerExpand(record, event);
    }
    for (var _len = arguments.length, args = new Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
      args[_key - 1] = arguments[_key];
    }
    onRowClick === null || onRowClick === void 0 || onRowClick.apply(void 0, [event].concat(args));
  };

  // ====================== RowClassName ======================
  var computeRowClassName;
  if (typeof rowClassName === 'string') {
    computeRowClassName = rowClassName;
  } else if (typeof rowClassName === 'function') {
    computeRowClassName = rowClassName(record, recordIndex, indent);
  }

  // ========================= Column =========================
  var columnsKey = (0, _valueUtil.getColumnsKey)(flattenColumns);
  return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, context), {}, {
    columnsKey: columnsKey,
    nestExpandable: nestExpandable,
    expanded: expanded,
    hasNestChildren: hasNestChildren,
    record: record,
    onTriggerExpand: onInternalTriggerExpand,
    rowSupportExpand: rowSupportExpand,
    expandable: mergedExpandable,
    rowProps: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, rowProps), {}, {
      className: (0, _classnames.default)(computeRowClassName, rowProps === null || rowProps === void 0 ? void 0 : rowProps.className),
      onClick: onClick
    })
  });
}