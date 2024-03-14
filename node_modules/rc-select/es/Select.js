import _extends from "@babel/runtime/helpers/esm/extends";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _typeof from "@babel/runtime/helpers/esm/typeof";
var _excluded = ["id", "mode", "prefixCls", "backfill", "fieldNames", "inputValue", "searchValue", "onSearch", "autoClearSearchValue", "onSelect", "onDeselect", "dropdownMatchSelectWidth", "filterOption", "filterSort", "optionFilterProp", "optionLabelProp", "options", "optionRender", "children", "defaultActiveFirstOption", "menuItemSelectedIcon", "virtual", "direction", "listHeight", "listItemHeight", "labelRender", "value", "defaultValue", "labelInValue", "onChange", "maxCount"];
/**
 * To match accessibility requirement, we always provide an input in the component.
 * Other element will not set `tabIndex` to avoid `onBlur` sequence problem.
 * For focused select, we set `aria-live="polite"` to update the accessibility content.
 *
 * ref:
 * - keyboard: https://developer.mozilla.org/en-US/docs/Web/Accessibility/ARIA/Roles/listbox_role#Keyboard_interactions
 *
 * New api:
 * - listHeight
 * - listItemHeight
 * - component
 *
 * Remove deprecated api:
 * - multiple
 * - tags
 * - combobox
 * - firstActiveValue
 * - dropdownMenuStyle
 * - openClassName (Not list in api)
 *
 * Update:
 * - `backfill` only support `combobox` mode
 * - `combobox` mode not support `labelInValue` since it's meaningless
 * - `getInputElement` only support `combobox` mode
 * - `onChange` return OptionData instead of ReactNode
 * - `filterOption` `onChange` `onSelect` accept OptionData instead of ReactNode
 * - `combobox` mode trigger `onChange` will get `undefined` if no `value` match in Option
 * - `combobox` mode not support `optionLabelProp`
 */

import useMergedState from "rc-util/es/hooks/useMergedState";
import warning from "rc-util/es/warning";
import * as React from 'react';
import BaseSelect, { isMultiple } from "./BaseSelect";
import OptGroup from "./OptGroup";
import Option from "./Option";
import OptionList from "./OptionList";
import SelectContext from "./SelectContext";
import useCache from "./hooks/useCache";
import useFilterOptions from "./hooks/useFilterOptions";
import useId from "./hooks/useId";
import useOptions from "./hooks/useOptions";
import useRefFunc from "./hooks/useRefFunc";
import { hasValue, isComboNoValue, toArray } from "./utils/commonUtil";
import { fillFieldNames, flattenOptions, injectPropsWithOption } from "./utils/valueUtil";
import warningProps, { warningNullOptions } from "./utils/warningPropsUtil";
var OMIT_DOM_PROPS = ['inputValue'];
function isRawValue(value) {
  return !value || _typeof(value) !== 'object';
}
var Select = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var id = props.id,
    mode = props.mode,
    _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-select' : _props$prefixCls,
    backfill = props.backfill,
    fieldNames = props.fieldNames,
    inputValue = props.inputValue,
    searchValue = props.searchValue,
    onSearch = props.onSearch,
    _props$autoClearSearc = props.autoClearSearchValue,
    autoClearSearchValue = _props$autoClearSearc === void 0 ? true : _props$autoClearSearc,
    onSelect = props.onSelect,
    onDeselect = props.onDeselect,
    _props$dropdownMatchS = props.dropdownMatchSelectWidth,
    dropdownMatchSelectWidth = _props$dropdownMatchS === void 0 ? true : _props$dropdownMatchS,
    filterOption = props.filterOption,
    filterSort = props.filterSort,
    optionFilterProp = props.optionFilterProp,
    optionLabelProp = props.optionLabelProp,
    options = props.options,
    optionRender = props.optionRender,
    children = props.children,
    defaultActiveFirstOption = props.defaultActiveFirstOption,
    menuItemSelectedIcon = props.menuItemSelectedIcon,
    virtual = props.virtual,
    direction = props.direction,
    _props$listHeight = props.listHeight,
    listHeight = _props$listHeight === void 0 ? 200 : _props$listHeight,
    _props$listItemHeight = props.listItemHeight,
    listItemHeight = _props$listItemHeight === void 0 ? 20 : _props$listItemHeight,
    labelRender = props.labelRender,
    value = props.value,
    defaultValue = props.defaultValue,
    labelInValue = props.labelInValue,
    onChange = props.onChange,
    maxCount = props.maxCount,
    restProps = _objectWithoutProperties(props, _excluded);
  var mergedId = useId(id);
  var multiple = isMultiple(mode);
  var childrenAsData = !!(!options && children);
  var mergedFilterOption = React.useMemo(function () {
    if (filterOption === undefined && mode === 'combobox') {
      return false;
    }
    return filterOption;
  }, [filterOption, mode]);

  // ========================= FieldNames =========================
  var mergedFieldNames = React.useMemo(function () {
    return fillFieldNames(fieldNames, childrenAsData);
  }, /* eslint-disable react-hooks/exhaustive-deps */
  [
  // We stringify fieldNames to avoid unnecessary re-renders.
  JSON.stringify(fieldNames), childrenAsData]
  /* eslint-enable react-hooks/exhaustive-deps */);

  // =========================== Search ===========================
  var _useMergedState = useMergedState('', {
      value: searchValue !== undefined ? searchValue : inputValue,
      postState: function postState(search) {
        return search || '';
      }
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    mergedSearchValue = _useMergedState2[0],
    setSearchValue = _useMergedState2[1];

  // =========================== Option ===========================
  var parsedOptions = useOptions(options, children, mergedFieldNames, optionFilterProp, optionLabelProp);
  var valueOptions = parsedOptions.valueOptions,
    labelOptions = parsedOptions.labelOptions,
    mergedOptions = parsedOptions.options;

  // ========================= Wrap Value =========================
  var convert2LabelValues = React.useCallback(function (draftValues) {
    // Convert to array
    var valueList = toArray(draftValues);

    // Convert to labelInValue type
    return valueList.map(function (val) {
      var rawValue;
      var rawLabel;
      var rawKey;
      var rawDisabled;
      var rawTitle;

      // Fill label & value
      if (isRawValue(val)) {
        rawValue = val;
      } else {
        var _val$value;
        rawKey = val.key;
        rawLabel = val.label;
        rawValue = (_val$value = val.value) !== null && _val$value !== void 0 ? _val$value : rawKey;
      }
      var option = valueOptions.get(rawValue);
      if (option) {
        var _option$key;
        // Fill missing props
        if (rawLabel === undefined) rawLabel = option === null || option === void 0 ? void 0 : option[optionLabelProp || mergedFieldNames.label];
        if (rawKey === undefined) rawKey = (_option$key = option === null || option === void 0 ? void 0 : option.key) !== null && _option$key !== void 0 ? _option$key : rawValue;
        rawDisabled = option === null || option === void 0 ? void 0 : option.disabled;
        rawTitle = option === null || option === void 0 ? void 0 : option.title;

        // Warning if label not same as provided
        if (process.env.NODE_ENV !== 'production' && !optionLabelProp) {
          var optionLabel = option === null || option === void 0 ? void 0 : option[mergedFieldNames.label];
          if (optionLabel !== undefined && ! /*#__PURE__*/React.isValidElement(optionLabel) && ! /*#__PURE__*/React.isValidElement(rawLabel) && optionLabel !== rawLabel) {
            warning(false, '`label` of `value` is not same as `label` in Select options.');
          }
        }
      }
      return {
        label: rawLabel,
        value: rawValue,
        key: rawKey,
        disabled: rawDisabled,
        title: rawTitle
      };
    });
  }, [mergedFieldNames, optionLabelProp, valueOptions]);

  // =========================== Values ===========================
  var _useMergedState3 = useMergedState(defaultValue, {
      value: value
    }),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    internalValue = _useMergedState4[0],
    setInternalValue = _useMergedState4[1];

  // Merged value with LabelValueType
  var rawLabeledValues = React.useMemo(function () {
    var _values$;
    var newInternalValue = multiple && internalValue === null ? [] : internalValue;
    var values = convert2LabelValues(newInternalValue);

    // combobox no need save value when it's no value (exclude value equal 0)
    if (mode === 'combobox' && isComboNoValue((_values$ = values[0]) === null || _values$ === void 0 ? void 0 : _values$.value)) {
      return [];
    }
    return values;
  }, [internalValue, convert2LabelValues, mode, multiple]);

  // Fill label with cache to avoid option remove
  var _useCache = useCache(rawLabeledValues, valueOptions),
    _useCache2 = _slicedToArray(_useCache, 2),
    mergedValues = _useCache2[0],
    getMixedOption = _useCache2[1];
  var displayValues = React.useMemo(function () {
    // `null` need show as placeholder instead
    // https://github.com/ant-design/ant-design/issues/25057
    if (!mode && mergedValues.length === 1) {
      var firstValue = mergedValues[0];
      if (firstValue.value === null && (firstValue.label === null || firstValue.label === undefined)) {
        return [];
      }
    }
    return mergedValues.map(function (item) {
      var _ref;
      return _objectSpread(_objectSpread({}, item), {}, {
        label: (_ref = typeof labelRender === 'function' ? labelRender(item) : item.label) !== null && _ref !== void 0 ? _ref : item.value
      });
    });
  }, [mode, mergedValues, labelRender]);

  /** Convert `displayValues` to raw value type set */
  var rawValues = React.useMemo(function () {
    return new Set(mergedValues.map(function (val) {
      return val.value;
    }));
  }, [mergedValues]);
  React.useEffect(function () {
    if (mode === 'combobox') {
      var _mergedValues$;
      var strValue = (_mergedValues$ = mergedValues[0]) === null || _mergedValues$ === void 0 ? void 0 : _mergedValues$.value;
      setSearchValue(hasValue(strValue) ? String(strValue) : '');
    }
  }, [mergedValues]);

  // ======================= Display Option =======================
  // Create a placeholder item if not exist in `options`
  var createTagOption = useRefFunc(function (val, label) {
    var mergedLabel = label !== null && label !== void 0 ? label : val;
    return _defineProperty(_defineProperty({}, mergedFieldNames.value, val), mergedFieldNames.label, mergedLabel);
  });

  // Fill tag as option if mode is `tags`
  var filledTagOptions = React.useMemo(function () {
    if (mode !== 'tags') {
      return mergedOptions;
    }

    // >>> Tag mode
    var cloneOptions = _toConsumableArray(mergedOptions);

    // Check if value exist in options (include new patch item)
    var existOptions = function existOptions(val) {
      return valueOptions.has(val);
    };

    // Fill current value as option
    _toConsumableArray(mergedValues).sort(function (a, b) {
      return a.value < b.value ? -1 : 1;
    }).forEach(function (item) {
      var val = item.value;
      if (!existOptions(val)) {
        cloneOptions.push(createTagOption(val, item.label));
      }
    });
    return cloneOptions;
  }, [createTagOption, mergedOptions, valueOptions, mergedValues, mode]);
  var filteredOptions = useFilterOptions(filledTagOptions, mergedFieldNames, mergedSearchValue, mergedFilterOption, optionFilterProp);

  // Fill options with search value if needed
  var filledSearchOptions = React.useMemo(function () {
    if (mode !== 'tags' || !mergedSearchValue || filteredOptions.some(function (item) {
      return item[optionFilterProp || 'value'] === mergedSearchValue;
    })) {
      return filteredOptions;
    }
    // ignore when search value equal select input value
    if (filteredOptions.some(function (item) {
      return item[mergedFieldNames.value] === mergedSearchValue;
    })) {
      return filteredOptions;
    }
    // Fill search value as option
    return [createTagOption(mergedSearchValue)].concat(_toConsumableArray(filteredOptions));
  }, [createTagOption, optionFilterProp, mode, filteredOptions, mergedSearchValue, mergedFieldNames]);
  var orderedFilteredOptions = React.useMemo(function () {
    if (!filterSort) {
      return filledSearchOptions;
    }
    return _toConsumableArray(filledSearchOptions).sort(function (a, b) {
      return filterSort(a, b);
    });
  }, [filledSearchOptions, filterSort]);
  var displayOptions = React.useMemo(function () {
    return flattenOptions(orderedFilteredOptions, {
      fieldNames: mergedFieldNames,
      childrenAsData: childrenAsData
    });
  }, [orderedFilteredOptions, mergedFieldNames, childrenAsData]);

  // =========================== Change ===========================
  var triggerChange = function triggerChange(values) {
    var labeledValues = convert2LabelValues(values);
    setInternalValue(labeledValues);
    if (onChange && (
    // Trigger event only when value changed
    labeledValues.length !== mergedValues.length || labeledValues.some(function (newVal, index) {
      var _mergedValues$index;
      return ((_mergedValues$index = mergedValues[index]) === null || _mergedValues$index === void 0 ? void 0 : _mergedValues$index.value) !== (newVal === null || newVal === void 0 ? void 0 : newVal.value);
    }))) {
      var returnValues = labelInValue ? labeledValues : labeledValues.map(function (v) {
        return v.value;
      });
      var returnOptions = labeledValues.map(function (v) {
        return injectPropsWithOption(getMixedOption(v.value));
      });
      onChange(
      // Value
      multiple ? returnValues : returnValues[0],
      // Option
      multiple ? returnOptions : returnOptions[0]);
    }
  };

  // ======================= Accessibility ========================
  var _React$useState = React.useState(null),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    activeValue = _React$useState2[0],
    setActiveValue = _React$useState2[1];
  var _React$useState3 = React.useState(0),
    _React$useState4 = _slicedToArray(_React$useState3, 2),
    accessibilityIndex = _React$useState4[0],
    setAccessibilityIndex = _React$useState4[1];
  var mergedDefaultActiveFirstOption = defaultActiveFirstOption !== undefined ? defaultActiveFirstOption : mode !== 'combobox';
  var onActiveValue = React.useCallback(function (active, index) {
    var _ref3 = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {},
      _ref3$source = _ref3.source,
      source = _ref3$source === void 0 ? 'keyboard' : _ref3$source;
    setAccessibilityIndex(index);
    if (backfill && mode === 'combobox' && active !== null && source === 'keyboard') {
      setActiveValue(String(active));
    }
  }, [backfill, mode]);

  // ========================= OptionList =========================
  var triggerSelect = function triggerSelect(val, selected, type) {
    var getSelectEnt = function getSelectEnt() {
      var _option$key2;
      var option = getMixedOption(val);
      return [labelInValue ? {
        label: option === null || option === void 0 ? void 0 : option[mergedFieldNames.label],
        value: val,
        key: (_option$key2 = option === null || option === void 0 ? void 0 : option.key) !== null && _option$key2 !== void 0 ? _option$key2 : val
      } : val, injectPropsWithOption(option)];
    };
    if (selected && onSelect) {
      var _getSelectEnt = getSelectEnt(),
        _getSelectEnt2 = _slicedToArray(_getSelectEnt, 2),
        wrappedValue = _getSelectEnt2[0],
        _option = _getSelectEnt2[1];
      onSelect(wrappedValue, _option);
    } else if (!selected && onDeselect && type !== 'clear') {
      var _getSelectEnt3 = getSelectEnt(),
        _getSelectEnt4 = _slicedToArray(_getSelectEnt3, 2),
        _wrappedValue = _getSelectEnt4[0],
        _option2 = _getSelectEnt4[1];
      onDeselect(_wrappedValue, _option2);
    }
  };

  // Used for OptionList selection
  var onInternalSelect = useRefFunc(function (val, info) {
    var cloneValues;

    // Single mode always trigger select only with option list
    var mergedSelect = multiple ? info.selected : true;
    if (mergedSelect) {
      cloneValues = multiple ? [].concat(_toConsumableArray(mergedValues), [val]) : [val];
    } else {
      cloneValues = mergedValues.filter(function (v) {
        return v.value !== val;
      });
    }
    triggerChange(cloneValues);
    triggerSelect(val, mergedSelect);

    // Clean search value if single or configured
    if (mode === 'combobox') {
      // setSearchValue(String(val));
      setActiveValue('');
    } else if (!isMultiple || autoClearSearchValue) {
      setSearchValue('');
      setActiveValue('');
    }
  });

  // ======================= Display Change =======================
  // BaseSelect display values change
  var onDisplayValuesChange = function onDisplayValuesChange(nextValues, info) {
    triggerChange(nextValues);
    var type = info.type,
      values = info.values;
    if (type === 'remove' || type === 'clear') {
      values.forEach(function (item) {
        triggerSelect(item.value, false, type);
      });
    }
  };

  // =========================== Search ===========================
  var onInternalSearch = function onInternalSearch(searchText, info) {
    setSearchValue(searchText);
    setActiveValue(null);

    // [Submit] Tag mode should flush input
    if (info.source === 'submit') {
      var formatted = (searchText || '').trim();
      // prevent empty tags from appearing when you click the Enter button
      if (formatted) {
        var newRawValues = Array.from(new Set([].concat(_toConsumableArray(rawValues), [formatted])));
        triggerChange(newRawValues);
        triggerSelect(formatted, true);
        setSearchValue('');
      }
      return;
    }
    if (info.source !== 'blur') {
      if (mode === 'combobox') {
        triggerChange(searchText);
      }
      onSearch === null || onSearch === void 0 || onSearch(searchText);
    }
  };
  var onInternalSearchSplit = function onInternalSearchSplit(words) {
    var patchValues = words;
    if (mode !== 'tags') {
      patchValues = words.map(function (word) {
        var opt = labelOptions.get(word);
        return opt === null || opt === void 0 ? void 0 : opt.value;
      }).filter(function (val) {
        return val !== undefined;
      });
    }
    var newRawValues = Array.from(new Set([].concat(_toConsumableArray(rawValues), _toConsumableArray(patchValues))));
    triggerChange(newRawValues);
    newRawValues.forEach(function (newRawValue) {
      triggerSelect(newRawValue, true);
    });
  };

  // ========================== Context ===========================
  var selectContext = React.useMemo(function () {
    var realVirtual = virtual !== false && dropdownMatchSelectWidth !== false;
    return _objectSpread(_objectSpread({}, parsedOptions), {}, {
      flattenOptions: displayOptions,
      onActiveValue: onActiveValue,
      defaultActiveFirstOption: mergedDefaultActiveFirstOption,
      onSelect: onInternalSelect,
      menuItemSelectedIcon: menuItemSelectedIcon,
      rawValues: rawValues,
      fieldNames: mergedFieldNames,
      virtual: realVirtual,
      direction: direction,
      listHeight: listHeight,
      listItemHeight: listItemHeight,
      childrenAsData: childrenAsData,
      maxCount: maxCount,
      optionRender: optionRender
    });
  }, [maxCount, parsedOptions, displayOptions, onActiveValue, mergedDefaultActiveFirstOption, onInternalSelect, menuItemSelectedIcon, rawValues, mergedFieldNames, virtual, dropdownMatchSelectWidth, direction, listHeight, listItemHeight, childrenAsData, optionRender]);

  // ========================== Warning ===========================
  if (process.env.NODE_ENV !== 'production') {
    warningProps(props);
    warningNullOptions(mergedOptions, mergedFieldNames);
  }

  // ==============================================================
  // ==                          Render                          ==
  // ==============================================================
  return /*#__PURE__*/React.createElement(SelectContext.Provider, {
    value: selectContext
  }, /*#__PURE__*/React.createElement(BaseSelect, _extends({}, restProps, {
    // >>> MISC
    id: mergedId,
    prefixCls: prefixCls,
    ref: ref,
    omitDomProps: OMIT_DOM_PROPS,
    mode: mode
    // >>> Values
    ,
    displayValues: displayValues,
    onDisplayValuesChange: onDisplayValuesChange
    // >>> Trigger
    ,
    direction: direction
    // >>> Search
    ,
    searchValue: mergedSearchValue,
    onSearch: onInternalSearch,
    autoClearSearchValue: autoClearSearchValue,
    onSearchSplit: onInternalSearchSplit,
    dropdownMatchSelectWidth: dropdownMatchSelectWidth
    // >>> OptionList
    ,
    OptionList: OptionList,
    emptyOptions: !displayOptions.length
    // >>> Accessibility
    ,
    activeValue: activeValue,
    activeDescendantId: "".concat(mergedId, "_list_").concat(accessibilityIndex)
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Select.displayName = 'Select';
}
var TypedSelect = Select;
TypedSelect.Option = Option;
TypedSelect.OptGroup = OptGroup;
export default TypedSelect;