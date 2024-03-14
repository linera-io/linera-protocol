"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.SELECTION_NONE = exports.SELECTION_INVERT = exports.SELECTION_COLUMN = exports.SELECTION_ALL = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _DownOutlined = _interopRequireDefault(require("@ant-design/icons/DownOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcTable = require("rc-table");
var _util = require("rc-tree/lib/util");
var _conductUtil = require("rc-tree/lib/utils/conductUtil");
var _treeUtil = require("rc-tree/lib/utils/treeUtil");
var _useMergedState = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var _useMultipleSelect = _interopRequireDefault(require("../../_util/hooks/useMultipleSelect"));
var _warning = require("../../_util/warning");
var _checkbox = _interopRequireDefault(require("../../checkbox"));
var _dropdown = _interopRequireDefault(require("../../dropdown"));
var _radio = _interopRequireDefault(require("../../radio"));
// TODO: warning if use ajax!!!
const SELECTION_COLUMN = exports.SELECTION_COLUMN = {};
const SELECTION_ALL = exports.SELECTION_ALL = 'SELECT_ALL';
const SELECTION_INVERT = exports.SELECTION_INVERT = 'SELECT_INVERT';
const SELECTION_NONE = exports.SELECTION_NONE = 'SELECT_NONE';
const EMPTY_LIST = [];
const flattenData = (childrenColumnName, data) => {
  let list = [];
  (data || []).forEach(record => {
    list.push(record);
    if (record && typeof record === 'object' && childrenColumnName in record) {
      list = [].concat((0, _toConsumableArray2.default)(list), (0, _toConsumableArray2.default)(flattenData(childrenColumnName, record[childrenColumnName])));
    }
  });
  return list;
};
const useSelection = (config, rowSelection) => {
  const {
    preserveSelectedRowKeys,
    selectedRowKeys,
    defaultSelectedRowKeys,
    getCheckboxProps,
    onChange: onSelectionChange,
    onSelect,
    onSelectAll,
    onSelectInvert,
    onSelectNone,
    onSelectMultiple,
    columnWidth: selectionColWidth,
    type: selectionType,
    selections,
    fixed,
    renderCell: customizeRenderCell,
    hideSelectAll,
    checkStrictly = true
  } = rowSelection || {};
  const {
    prefixCls,
    data,
    pageData,
    getRecordByKey,
    getRowKey,
    expandType,
    childrenColumnName,
    locale: tableLocale,
    getPopupContainer
  } = config;
  const warning = (0, _warning.devUseWarning)('Table');
  // ========================= MultipleSelect =========================
  const [multipleSelect, updatePrevSelectedIndex] = (0, _useMultipleSelect.default)(item => item);
  // ========================= Keys =========================
  const [mergedSelectedKeys, setMergedSelectedKeys] = (0, _useMergedState.default)(selectedRowKeys || defaultSelectedRowKeys || EMPTY_LIST, {
    value: selectedRowKeys
  });
  // ======================== Caches ========================
  const preserveRecordsRef = React.useRef(new Map());
  const updatePreserveRecordsCache = (0, _react.useCallback)(keys => {
    if (preserveSelectedRowKeys) {
      const newCache = new Map();
      // Keep key if mark as preserveSelectedRowKeys
      keys.forEach(key => {
        let record = getRecordByKey(key);
        if (!record && preserveRecordsRef.current.has(key)) {
          record = preserveRecordsRef.current.get(key);
        }
        newCache.set(key, record);
      });
      // Refresh to new cache
      preserveRecordsRef.current = newCache;
    }
  }, [getRecordByKey, preserveSelectedRowKeys]);
  // Update cache with selectedKeys
  React.useEffect(() => {
    updatePreserveRecordsCache(mergedSelectedKeys);
  }, [mergedSelectedKeys]);
  const {
    keyEntities
  } = (0, _react.useMemo)(() => {
    if (checkStrictly) {
      return {
        keyEntities: null
      };
    }
    let convertData = data;
    if (preserveSelectedRowKeys) {
      const keysSet = new Set(data.map((record, index) => getRowKey(record, index)));
      // remove preserveRecords that duplicate data
      const preserveRecords = Array.from(preserveRecordsRef.current).reduce((total, _ref) => {
        let [key, value] = _ref;
        return keysSet.has(key) ? total : total.concat(value);
      }, []);
      convertData = [].concat((0, _toConsumableArray2.default)(convertData), (0, _toConsumableArray2.default)(preserveRecords));
    }
    return (0, _treeUtil.convertDataToEntities)(convertData, {
      externalGetKey: getRowKey,
      childrenPropName: childrenColumnName
    });
  }, [data, getRowKey, checkStrictly, childrenColumnName, preserveSelectedRowKeys]);
  // Get flatten data
  const flattedData = (0, _react.useMemo)(() => flattenData(childrenColumnName, pageData), [childrenColumnName, pageData]);
  // Get all checkbox props
  const checkboxPropsMap = (0, _react.useMemo)(() => {
    const map = new Map();
    flattedData.forEach((record, index) => {
      const key = getRowKey(record, index);
      const checkboxProps = (getCheckboxProps ? getCheckboxProps(record) : null) || {};
      map.set(key, checkboxProps);
      process.env.NODE_ENV !== "production" ? warning(!('checked' in checkboxProps || 'defaultChecked' in checkboxProps), 'usage', 'Do not set `checked` or `defaultChecked` in `getCheckboxProps`. Please use `selectedRowKeys` instead.') : void 0;
    });
    return map;
  }, [flattedData, getRowKey, getCheckboxProps]);
  const isCheckboxDisabled = (0, _react.useCallback)(r => {
    var _a;
    return !!((_a = checkboxPropsMap.get(getRowKey(r))) === null || _a === void 0 ? void 0 : _a.disabled);
  }, [checkboxPropsMap, getRowKey]);
  const [derivedSelectedKeys, derivedHalfSelectedKeys] = (0, _react.useMemo)(() => {
    if (checkStrictly) {
      return [mergedSelectedKeys || [], []];
    }
    const {
      checkedKeys,
      halfCheckedKeys
    } = (0, _conductUtil.conductCheck)(mergedSelectedKeys, true, keyEntities, isCheckboxDisabled);
    return [checkedKeys || [], halfCheckedKeys];
  }, [mergedSelectedKeys, checkStrictly, keyEntities, isCheckboxDisabled]);
  const derivedSelectedKeySet = (0, _react.useMemo)(() => {
    const keys = selectionType === 'radio' ? derivedSelectedKeys.slice(0, 1) : derivedSelectedKeys;
    return new Set(keys);
  }, [derivedSelectedKeys, selectionType]);
  const derivedHalfSelectedKeySet = (0, _react.useMemo)(() => selectionType === 'radio' ? new Set() : new Set(derivedHalfSelectedKeys), [derivedHalfSelectedKeys, selectionType]);
  // Reset if rowSelection reset
  React.useEffect(() => {
    if (!rowSelection) {
      setMergedSelectedKeys(EMPTY_LIST);
    }
  }, [!!rowSelection]);
  const setSelectedKeys = (0, _react.useCallback)((keys, method) => {
    let availableKeys;
    let records;
    updatePreserveRecordsCache(keys);
    if (preserveSelectedRowKeys) {
      availableKeys = keys;
      records = keys.map(key => preserveRecordsRef.current.get(key));
    } else {
      // Filter key which not exist in the `dataSource`
      availableKeys = [];
      records = [];
      keys.forEach(key => {
        const record = getRecordByKey(key);
        if (record !== undefined) {
          availableKeys.push(key);
          records.push(record);
        }
      });
    }
    setMergedSelectedKeys(availableKeys);
    onSelectionChange === null || onSelectionChange === void 0 ? void 0 : onSelectionChange(availableKeys, records, {
      type: method
    });
  }, [setMergedSelectedKeys, getRecordByKey, onSelectionChange, preserveSelectedRowKeys]);
  // ====================== Selections ======================
  // Trigger single `onSelect` event
  const triggerSingleSelection = (0, _react.useCallback)((key, selected, keys, event) => {
    if (onSelect) {
      const rows = keys.map(k => getRecordByKey(k));
      onSelect(getRecordByKey(key), selected, rows, event);
    }
    setSelectedKeys(keys, 'single');
  }, [onSelect, getRecordByKey, setSelectedKeys]);
  const mergedSelections = (0, _react.useMemo)(() => {
    if (!selections || hideSelectAll) {
      return null;
    }
    const selectionList = selections === true ? [SELECTION_ALL, SELECTION_INVERT, SELECTION_NONE] : selections;
    return selectionList.map(selection => {
      if (selection === SELECTION_ALL) {
        return {
          key: 'all',
          text: tableLocale.selectionAll,
          onSelect() {
            setSelectedKeys(data.map((record, index) => getRowKey(record, index)).filter(key => {
              const checkProps = checkboxPropsMap.get(key);
              return !(checkProps === null || checkProps === void 0 ? void 0 : checkProps.disabled) || derivedSelectedKeySet.has(key);
            }), 'all');
          }
        };
      }
      if (selection === SELECTION_INVERT) {
        return {
          key: 'invert',
          text: tableLocale.selectInvert,
          onSelect() {
            const keySet = new Set(derivedSelectedKeySet);
            pageData.forEach((record, index) => {
              const key = getRowKey(record, index);
              const checkProps = checkboxPropsMap.get(key);
              if (!(checkProps === null || checkProps === void 0 ? void 0 : checkProps.disabled)) {
                if (keySet.has(key)) {
                  keySet.delete(key);
                } else {
                  keySet.add(key);
                }
              }
            });
            const keys = Array.from(keySet);
            if (onSelectInvert) {
              warning.deprecated(false, 'onSelectInvert', 'onChange');
              onSelectInvert(keys);
            }
            setSelectedKeys(keys, 'invert');
          }
        };
      }
      if (selection === SELECTION_NONE) {
        return {
          key: 'none',
          text: tableLocale.selectNone,
          onSelect() {
            onSelectNone === null || onSelectNone === void 0 ? void 0 : onSelectNone();
            setSelectedKeys(Array.from(derivedSelectedKeySet).filter(key => {
              const checkProps = checkboxPropsMap.get(key);
              return checkProps === null || checkProps === void 0 ? void 0 : checkProps.disabled;
            }), 'none');
          }
        };
      }
      return selection;
    }).map(selection => Object.assign(Object.assign({}, selection), {
      onSelect: function () {
        var _a2;
        var _a;
        for (var _len = arguments.length, rest = new Array(_len), _key = 0; _key < _len; _key++) {
          rest[_key] = arguments[_key];
        }
        (_a = selection.onSelect) === null || _a === void 0 ? void 0 : (_a2 = _a).call.apply(_a2, [selection].concat(rest));
        updatePrevSelectedIndex(null);
      }
    }));
  }, [selections, derivedSelectedKeySet, pageData, getRowKey, onSelectInvert, setSelectedKeys]);
  // ======================= Columns ========================
  const transformColumns = (0, _react.useCallback)(columns => {
    var _a;
    // >>>>>>>>>>> Skip if not exists `rowSelection`
    if (!rowSelection) {
      process.env.NODE_ENV !== "production" ? warning(!columns.includes(SELECTION_COLUMN), 'usage', '`rowSelection` is not config but `SELECTION_COLUMN` exists in the `columns`.') : void 0;
      return columns.filter(col => col !== SELECTION_COLUMN);
    }
    // >>>>>>>>>>> Support selection
    let cloneColumns = (0, _toConsumableArray2.default)(columns);
    const keySet = new Set(derivedSelectedKeySet);
    // Record key only need check with enabled
    const recordKeys = flattedData.map(getRowKey).filter(key => !checkboxPropsMap.get(key).disabled);
    const checkedCurrentAll = recordKeys.every(key => keySet.has(key));
    const checkedCurrentSome = recordKeys.some(key => keySet.has(key));
    const onSelectAllChange = () => {
      const changeKeys = [];
      if (checkedCurrentAll) {
        recordKeys.forEach(key => {
          keySet.delete(key);
          changeKeys.push(key);
        });
      } else {
        recordKeys.forEach(key => {
          if (!keySet.has(key)) {
            keySet.add(key);
            changeKeys.push(key);
          }
        });
      }
      const keys = Array.from(keySet);
      onSelectAll === null || onSelectAll === void 0 ? void 0 : onSelectAll(!checkedCurrentAll, keys.map(k => getRecordByKey(k)), changeKeys.map(k => getRecordByKey(k)));
      setSelectedKeys(keys, 'all');
      updatePrevSelectedIndex(null);
    };
    // ===================== Render =====================
    // Title Cell
    let title;
    let columnTitleCheckbox;
    if (selectionType !== 'radio') {
      let customizeSelections;
      if (mergedSelections) {
        const menu = {
          getPopupContainer,
          items: mergedSelections.map((selection, index) => {
            const {
              key,
              text,
              onSelect: onSelectionClick
            } = selection;
            return {
              key: key !== null && key !== void 0 ? key : index,
              onClick: () => {
                onSelectionClick === null || onSelectionClick === void 0 ? void 0 : onSelectionClick(recordKeys);
              },
              label: text
            };
          })
        };
        customizeSelections = /*#__PURE__*/React.createElement("div", {
          className: `${prefixCls}-selection-extra`
        }, /*#__PURE__*/React.createElement(_dropdown.default, {
          menu: menu,
          getPopupContainer: getPopupContainer
        }, /*#__PURE__*/React.createElement("span", null, /*#__PURE__*/React.createElement(_DownOutlined.default, null))));
      }
      const allDisabledData = flattedData.map((record, index) => {
        const key = getRowKey(record, index);
        const checkboxProps = checkboxPropsMap.get(key) || {};
        return Object.assign({
          checked: keySet.has(key)
        }, checkboxProps);
      }).filter(_ref2 => {
        let {
          disabled
        } = _ref2;
        return disabled;
      });
      const allDisabled = !!allDisabledData.length && allDisabledData.length === flattedData.length;
      const allDisabledAndChecked = allDisabled && allDisabledData.every(_ref3 => {
        let {
          checked
        } = _ref3;
        return checked;
      });
      const allDisabledSomeChecked = allDisabled && allDisabledData.some(_ref4 => {
        let {
          checked
        } = _ref4;
        return checked;
      });
      columnTitleCheckbox = /*#__PURE__*/React.createElement(_checkbox.default, {
        checked: !allDisabled ? !!flattedData.length && checkedCurrentAll : allDisabledAndChecked,
        indeterminate: !allDisabled ? !checkedCurrentAll && checkedCurrentSome : !allDisabledAndChecked && allDisabledSomeChecked,
        onChange: onSelectAllChange,
        disabled: flattedData.length === 0 || allDisabled,
        "aria-label": customizeSelections ? 'Custom selection' : 'Select all',
        skipGroup: true
      });
      title = !hideSelectAll && ( /*#__PURE__*/React.createElement("div", {
        className: `${prefixCls}-selection`
      }, columnTitleCheckbox, customizeSelections));
    }
    // Body Cell
    let renderCell;
    if (selectionType === 'radio') {
      renderCell = (_, record, index) => {
        const key = getRowKey(record, index);
        const checked = keySet.has(key);
        return {
          node: ( /*#__PURE__*/React.createElement(_radio.default, Object.assign({}, checkboxPropsMap.get(key), {
            checked: checked,
            onClick: e => e.stopPropagation(),
            onChange: event => {
              if (!keySet.has(key)) {
                triggerSingleSelection(key, true, [key], event.nativeEvent);
              }
            }
          }))),
          checked
        };
      };
    } else {
      renderCell = (_, record, index) => {
        var _a;
        const key = getRowKey(record, index);
        const checked = keySet.has(key);
        const indeterminate = derivedHalfSelectedKeySet.has(key);
        const checkboxProps = checkboxPropsMap.get(key);
        let mergedIndeterminate;
        if (expandType === 'nest') {
          mergedIndeterminate = indeterminate;
          process.env.NODE_ENV !== "production" ? warning(typeof (checkboxProps === null || checkboxProps === void 0 ? void 0 : checkboxProps.indeterminate) !== 'boolean', 'usage', 'set `indeterminate` using `rowSelection.getCheckboxProps` is not allowed with tree structured dataSource.') : void 0;
        } else {
          mergedIndeterminate = (_a = checkboxProps === null || checkboxProps === void 0 ? void 0 : checkboxProps.indeterminate) !== null && _a !== void 0 ? _a : indeterminate;
        }
        // Record checked
        return {
          node: ( /*#__PURE__*/React.createElement(_checkbox.default, Object.assign({}, checkboxProps, {
            indeterminate: mergedIndeterminate,
            checked: checked,
            skipGroup: true,
            onClick: e => e.stopPropagation(),
            onChange: _ref5 => {
              let {
                nativeEvent
              } = _ref5;
              const {
                shiftKey
              } = nativeEvent;
              const currentSelectedIndex = recordKeys.findIndex(item => item === key);
              const isMultiple = derivedSelectedKeys.some(item => recordKeys.includes(item));
              if (shiftKey && checkStrictly && isMultiple) {
                const changedKeys = multipleSelect(currentSelectedIndex, recordKeys, keySet);
                const keys = Array.from(keySet);
                onSelectMultiple === null || onSelectMultiple === void 0 ? void 0 : onSelectMultiple(!checked, keys.map(recordKey => getRecordByKey(recordKey)), changedKeys.map(recordKey => getRecordByKey(recordKey)));
                setSelectedKeys(keys, 'multiple');
              } else {
                // Single record selected
                const originCheckedKeys = derivedSelectedKeys;
                if (checkStrictly) {
                  const checkedKeys = checked ? (0, _util.arrDel)(originCheckedKeys, key) : (0, _util.arrAdd)(originCheckedKeys, key);
                  triggerSingleSelection(key, !checked, checkedKeys, nativeEvent);
                } else {
                  // Always fill first
                  const result = (0, _conductUtil.conductCheck)([].concat((0, _toConsumableArray2.default)(originCheckedKeys), [key]), true, keyEntities, isCheckboxDisabled);
                  const {
                    checkedKeys,
                    halfCheckedKeys
                  } = result;
                  let nextCheckedKeys = checkedKeys;
                  // If remove, we do it again to correction
                  if (checked) {
                    const tempKeySet = new Set(checkedKeys);
                    tempKeySet.delete(key);
                    nextCheckedKeys = (0, _conductUtil.conductCheck)(Array.from(tempKeySet), {
                      checked: false,
                      halfCheckedKeys
                    }, keyEntities, isCheckboxDisabled).checkedKeys;
                  }
                  triggerSingleSelection(key, !checked, nextCheckedKeys, nativeEvent);
                }
              }
              if (checked) {
                updatePrevSelectedIndex(null);
              } else {
                updatePrevSelectedIndex(currentSelectedIndex);
              }
            }
          }))),
          checked
        };
      };
    }
    const renderSelectionCell = (_, record, index) => {
      const {
        node,
        checked
      } = renderCell(_, record, index);
      if (customizeRenderCell) {
        return customizeRenderCell(checked, record, index, node);
      }
      return node;
    };
    // Insert selection column if not exist
    if (!cloneColumns.includes(SELECTION_COLUMN)) {
      // Always after expand icon
      if (cloneColumns.findIndex(col => {
        var _a;
        return ((_a = col[_rcTable.INTERNAL_COL_DEFINE]) === null || _a === void 0 ? void 0 : _a.columnType) === 'EXPAND_COLUMN';
      }) === 0) {
        const [expandColumn, ...restColumns] = cloneColumns;
        cloneColumns = [expandColumn, SELECTION_COLUMN].concat((0, _toConsumableArray2.default)(restColumns));
      } else {
        // Normal insert at first column
        cloneColumns = [SELECTION_COLUMN].concat((0, _toConsumableArray2.default)(cloneColumns));
      }
    }
    // Deduplicate selection column
    const selectionColumnIndex = cloneColumns.indexOf(SELECTION_COLUMN);
    process.env.NODE_ENV !== "production" ? warning(cloneColumns.filter(col => col === SELECTION_COLUMN).length <= 1, 'usage', 'Multiple `SELECTION_COLUMN` exist in `columns`.') : void 0;
    cloneColumns = cloneColumns.filter((column, index) => column !== SELECTION_COLUMN || index === selectionColumnIndex);
    // Fixed column logic
    const prevCol = cloneColumns[selectionColumnIndex - 1];
    const nextCol = cloneColumns[selectionColumnIndex + 1];
    let mergedFixed = fixed;
    if (mergedFixed === undefined) {
      if ((nextCol === null || nextCol === void 0 ? void 0 : nextCol.fixed) !== undefined) {
        mergedFixed = nextCol.fixed;
      } else if ((prevCol === null || prevCol === void 0 ? void 0 : prevCol.fixed) !== undefined) {
        mergedFixed = prevCol.fixed;
      }
    }
    if (mergedFixed && prevCol && ((_a = prevCol[_rcTable.INTERNAL_COL_DEFINE]) === null || _a === void 0 ? void 0 : _a.columnType) === 'EXPAND_COLUMN' && prevCol.fixed === undefined) {
      prevCol.fixed = mergedFixed;
    }
    const columnCls = (0, _classnames.default)(`${prefixCls}-selection-col`, {
      [`${prefixCls}-selection-col-with-dropdown`]: selections && selectionType === 'checkbox'
    });
    const renderColumnTitle = () => {
      if (!(rowSelection === null || rowSelection === void 0 ? void 0 : rowSelection.columnTitle)) {
        return title;
      }
      if (typeof rowSelection.columnTitle === 'function') {
        return rowSelection.columnTitle(columnTitleCheckbox);
      }
      return rowSelection.columnTitle;
    };
    // Replace with real selection column
    const selectionColumn = {
      fixed: mergedFixed,
      width: selectionColWidth,
      className: `${prefixCls}-selection-column`,
      title: renderColumnTitle(),
      render: renderSelectionCell,
      onCell: rowSelection.onCell,
      [_rcTable.INTERNAL_COL_DEFINE]: {
        className: columnCls
      }
    };
    return cloneColumns.map(col => col === SELECTION_COLUMN ? selectionColumn : col);
  }, [getRowKey, flattedData, rowSelection, derivedSelectedKeys, derivedSelectedKeySet, derivedHalfSelectedKeySet, selectionColWidth, mergedSelections, expandType, checkboxPropsMap, onSelectMultiple, triggerSingleSelection, isCheckboxDisabled]);
  return [transformColumns, derivedSelectedKeySet];
};
var _default = exports.default = useSelection;