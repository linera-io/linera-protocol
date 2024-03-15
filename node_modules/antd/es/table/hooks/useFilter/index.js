"use client";

import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import * as React from 'react';
import { devUseWarning } from '../../../_util/warning';
import { getColumnKey, getColumnPos, renderColumnTitle } from '../../util';
import FilterDropdown, { flattenKeys } from './FilterDropdown';
function collectFilterStates(columns, init, pos) {
  let filterStates = [];
  (columns || []).forEach((column, index) => {
    var _a;
    const columnPos = getColumnPos(index, pos);
    if (column.filters || 'filterDropdown' in column || 'onFilter' in column) {
      if ('filteredValue' in column) {
        // Controlled
        let filteredValues = column.filteredValue;
        if (!('filterDropdown' in column)) {
          filteredValues = (_a = filteredValues === null || filteredValues === void 0 ? void 0 : filteredValues.map(String)) !== null && _a !== void 0 ? _a : filteredValues;
        }
        filterStates.push({
          column,
          key: getColumnKey(column, columnPos),
          filteredKeys: filteredValues,
          forceFiltered: column.filtered
        });
      } else {
        // Uncontrolled
        filterStates.push({
          column,
          key: getColumnKey(column, columnPos),
          filteredKeys: init && column.defaultFilteredValue ? column.defaultFilteredValue : undefined,
          forceFiltered: column.filtered
        });
      }
    }
    if ('children' in column) {
      filterStates = [].concat(_toConsumableArray(filterStates), _toConsumableArray(collectFilterStates(column.children, init, columnPos)));
    }
  });
  return filterStates;
}
function injectFilter(prefixCls, dropdownPrefixCls, columns, filterStates, locale, triggerFilter, getPopupContainer, pos, rootClassName) {
  return columns.map((column, index) => {
    const columnPos = getColumnPos(index, pos);
    const {
      filterOnClose = true,
      filterMultiple = true,
      filterMode,
      filterSearch
    } = column;
    let newColumn = column;
    if (newColumn.filters || newColumn.filterDropdown) {
      const columnKey = getColumnKey(newColumn, columnPos);
      const filterState = filterStates.find(_ref => {
        let {
          key
        } = _ref;
        return columnKey === key;
      });
      newColumn = Object.assign(Object.assign({}, newColumn), {
        title: renderProps => ( /*#__PURE__*/React.createElement(FilterDropdown, {
          tablePrefixCls: prefixCls,
          prefixCls: `${prefixCls}-filter`,
          dropdownPrefixCls: dropdownPrefixCls,
          column: newColumn,
          columnKey: columnKey,
          filterState: filterState,
          filterOnClose: filterOnClose,
          filterMultiple: filterMultiple,
          filterMode: filterMode,
          filterSearch: filterSearch,
          triggerFilter: triggerFilter,
          locale: locale,
          getPopupContainer: getPopupContainer,
          rootClassName: rootClassName
        }, renderColumnTitle(column.title, renderProps)))
      });
    }
    if ('children' in newColumn) {
      newColumn = Object.assign(Object.assign({}, newColumn), {
        children: injectFilter(prefixCls, dropdownPrefixCls, newColumn.children, filterStates, locale, triggerFilter, getPopupContainer, columnPos, rootClassName)
      });
    }
    return newColumn;
  });
}
function generateFilterInfo(filterStates) {
  const currentFilters = {};
  filterStates.forEach(_ref2 => {
    let {
      key,
      filteredKeys,
      column
    } = _ref2;
    const keyAsString = key;
    const {
      filters,
      filterDropdown
    } = column;
    if (filterDropdown) {
      currentFilters[keyAsString] = filteredKeys || null;
    } else if (Array.isArray(filteredKeys)) {
      const keys = flattenKeys(filters);
      currentFilters[keyAsString] = keys.filter(originKey => filteredKeys.includes(String(originKey)));
    } else {
      currentFilters[keyAsString] = null;
    }
  });
  return currentFilters;
}
export function getFilterData(data, filterStates, childrenColumnName) {
  return filterStates.reduce((currentData, filterState) => {
    const {
      column: {
        onFilter,
        filters
      },
      filteredKeys
    } = filterState;
    if (onFilter && filteredKeys && filteredKeys.length) {
      return currentData
      // shallow copy
      .map(record => Object.assign({}, record)).filter(record => filteredKeys.some(key => {
        const keys = flattenKeys(filters);
        const keyIndex = keys.findIndex(k => String(k) === String(key));
        const realKey = keyIndex !== -1 ? keys[keyIndex] : key;
        // filter children
        if (record[childrenColumnName]) {
          record[childrenColumnName] = getFilterData(record[childrenColumnName], filterStates, childrenColumnName);
        }
        return onFilter(realKey, record);
      }));
    }
    return currentData;
  }, data);
}
const getMergedColumns = rawMergedColumns => rawMergedColumns.flatMap(column => {
  if ('children' in column) {
    return [column].concat(_toConsumableArray(getMergedColumns(column.children || [])));
  }
  return [column];
});
function useFilter(_ref3) {
  let {
    prefixCls,
    dropdownPrefixCls,
    mergedColumns: rawMergedColumns,
    onFilterChange,
    getPopupContainer,
    locale: tableLocale,
    rootClassName
  } = _ref3;
  const warning = devUseWarning('Table');
  const mergedColumns = React.useMemo(() => getMergedColumns(rawMergedColumns || []), [rawMergedColumns]);
  const [filterStates, setFilterStates] = React.useState(() => collectFilterStates(mergedColumns, true));
  const mergedFilterStates = React.useMemo(() => {
    const collectedStates = collectFilterStates(mergedColumns, false);
    if (collectedStates.length === 0) {
      return collectedStates;
    }
    let filteredKeysIsAllNotControlled = true;
    let filteredKeysIsAllControlled = true;
    collectedStates.forEach(_ref4 => {
      let {
        filteredKeys
      } = _ref4;
      if (filteredKeys !== undefined) {
        filteredKeysIsAllNotControlled = false;
      } else {
        filteredKeysIsAllControlled = false;
      }
    });
    // Return if not controlled
    if (filteredKeysIsAllNotControlled) {
      // Filter column may have been removed
      const keyList = (mergedColumns || []).map((column, index) => getColumnKey(column, getColumnPos(index)));
      return filterStates.filter(_ref5 => {
        let {
          key
        } = _ref5;
        return keyList.includes(key);
      }).map(item => {
        const col = mergedColumns[keyList.findIndex(key => key === item.key)];
        return Object.assign(Object.assign({}, item), {
          column: Object.assign(Object.assign({}, item.column), col),
          forceFiltered: col.filtered
        });
      });
    }
    process.env.NODE_ENV !== "production" ? warning(filteredKeysIsAllControlled, 'usage', 'Columns should all contain `filteredValue` or not contain `filteredValue`.') : void 0;
    return collectedStates;
  }, [mergedColumns, filterStates]);
  const filters = React.useMemo(() => generateFilterInfo(mergedFilterStates), [mergedFilterStates]);
  const triggerFilter = filterState => {
    const newFilterStates = mergedFilterStates.filter(_ref6 => {
      let {
        key
      } = _ref6;
      return key !== filterState.key;
    });
    newFilterStates.push(filterState);
    setFilterStates(newFilterStates);
    onFilterChange(generateFilterInfo(newFilterStates), newFilterStates);
  };
  const transformColumns = innerColumns => injectFilter(prefixCls, dropdownPrefixCls, innerColumns, mergedFilterStates, tableLocale, triggerFilter, getPopupContainer, undefined, rootClassName);
  return [transformColumns, mergedFilterStates, filters];
}
export { flattenKeys };
export default useFilter;