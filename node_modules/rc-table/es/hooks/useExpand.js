import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import warning from "rc-util/es/warning";
import * as React from 'react';
import { INTERNAL_HOOKS } from "../constant";
import { findAllChildrenKeys, renderExpandIcon } from "../utils/expandUtil";
import { getExpandableProps } from "../utils/legacyUtil";
export default function useExpand(props, mergedData, getRowKey) {
  var expandableConfig = getExpandableProps(props);
  var expandIcon = expandableConfig.expandIcon,
    expandedRowKeys = expandableConfig.expandedRowKeys,
    defaultExpandedRowKeys = expandableConfig.defaultExpandedRowKeys,
    defaultExpandAllRows = expandableConfig.defaultExpandAllRows,
    expandedRowRender = expandableConfig.expandedRowRender,
    onExpand = expandableConfig.onExpand,
    onExpandedRowsChange = expandableConfig.onExpandedRowsChange,
    childrenColumnName = expandableConfig.childrenColumnName;
  var mergedExpandIcon = expandIcon || renderExpandIcon;
  var mergedChildrenColumnName = childrenColumnName || 'children';
  var expandableType = React.useMemo(function () {
    if (expandedRowRender) {
      return 'row';
    }
    /* eslint-disable no-underscore-dangle */
    /**
     * Fix https://github.com/ant-design/ant-design/issues/21154
     * This is a workaround to not to break current behavior.
     * We can remove follow code after final release.
     *
     * To other developer:
     *  Do not use `__PARENT_RENDER_ICON__` in prod since we will remove this when refactor
     */
    if (props.expandable && props.internalHooks === INTERNAL_HOOKS && props.expandable.__PARENT_RENDER_ICON__ || mergedData.some(function (record) {
      return record && _typeof(record) === 'object' && record[mergedChildrenColumnName];
    })) {
      return 'nest';
    }
    /* eslint-enable */
    return false;
  }, [!!expandedRowRender, mergedData]);
  var _React$useState = React.useState(function () {
      if (defaultExpandedRowKeys) {
        return defaultExpandedRowKeys;
      }
      if (defaultExpandAllRows) {
        return findAllChildrenKeys(mergedData, getRowKey, mergedChildrenColumnName);
      }
      return [];
    }),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    innerExpandedKeys = _React$useState2[0],
    setInnerExpandedKeys = _React$useState2[1];
  var mergedExpandedKeys = React.useMemo(function () {
    return new Set(expandedRowKeys || innerExpandedKeys || []);
  }, [expandedRowKeys, innerExpandedKeys]);
  var onTriggerExpand = React.useCallback(function (record) {
    var key = getRowKey(record, mergedData.indexOf(record));
    var newExpandedKeys;
    var hasKey = mergedExpandedKeys.has(key);
    if (hasKey) {
      mergedExpandedKeys.delete(key);
      newExpandedKeys = _toConsumableArray(mergedExpandedKeys);
    } else {
      newExpandedKeys = [].concat(_toConsumableArray(mergedExpandedKeys), [key]);
    }
    setInnerExpandedKeys(newExpandedKeys);
    if (onExpand) {
      onExpand(!hasKey, record);
    }
    if (onExpandedRowsChange) {
      onExpandedRowsChange(newExpandedKeys);
    }
  }, [getRowKey, mergedExpandedKeys, mergedData, onExpand, onExpandedRowsChange]);

  // Warning if use `expandedRowRender` and nest children in the same time
  if (process.env.NODE_ENV !== 'production' && expandedRowRender && mergedData.some(function (record) {
    return Array.isArray(record === null || record === void 0 ? void 0 : record[mergedChildrenColumnName]);
  })) {
    warning(false, '`expandedRowRender` should not use with nested Table');
  }
  return [expandableConfig, expandableType, mergedExpandedKeys, mergedExpandIcon, mergedChildrenColumnName, onTriggerExpand];
}