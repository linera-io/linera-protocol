import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import { useContext } from '@rc-component/context';
import TableContext from "../context/TableContext";
import { getColumnsKey } from "../utils/valueUtil";
import { useEvent } from 'rc-util';
import classNames from 'classnames';
export default function useRowInfo(record, rowKey, recordIndex, indent) {
  var context = useContext(TableContext, ['prefixCls', 'fixedInfoList', 'flattenColumns', 'expandableType', 'expandRowByClick', 'onTriggerExpand', 'rowClassName', 'expandedRowClassName', 'indentSize', 'expandIcon', 'expandedRowRender', 'expandIconColumnIndex', 'expandedKeys', 'childrenColumnName', 'rowExpandable', 'onRow']);
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
  var onInternalTriggerExpand = useEvent(onTriggerExpand);

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
  var columnsKey = getColumnsKey(flattenColumns);
  return _objectSpread(_objectSpread({}, context), {}, {
    columnsKey: columnsKey,
    nestExpandable: nestExpandable,
    expanded: expanded,
    hasNestChildren: hasNestChildren,
    record: record,
    onTriggerExpand: onInternalTriggerExpand,
    rowSupportExpand: rowSupportExpand,
    expandable: mergedExpandable,
    rowProps: _objectSpread(_objectSpread({}, rowProps), {}, {
      className: classNames(computeRowClassName, rowProps === null || rowProps === void 0 ? void 0 : rowProps.className),
      onClick: onClick
    })
  });
}