import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["id", "prefixCls", "fieldNames", "defaultValue", "value", "changeOnSelect", "onChange", "displayRender", "checkable", "autoClearSearchValue", "searchValue", "onSearch", "showSearch", "expandTrigger", "options", "dropdownPrefixCls", "loadData", "popupVisible", "open", "popupClassName", "dropdownClassName", "dropdownMenuColumnStyle", "dropdownStyle", "popupPlacement", "placement", "onDropdownVisibleChange", "onPopupVisibleChange", "expandIcon", "loadingIcon", "children", "dropdownMatchSelectWidth", "showCheckedStrategy", "optionRender"];
import { BaseSelect } from 'rc-select';
import useId from "rc-select/es/hooks/useId";
import useEvent from "rc-util/es/hooks/useEvent";
import useMergedState from "rc-util/es/hooks/useMergedState";
import * as React from 'react';
import CascaderContext from "./context";
import useDisplayValues from "./hooks/useDisplayValues";
import useMissingValues from "./hooks/useMissingValues";
import useOptions from "./hooks/useOptions";
import useSearchConfig from "./hooks/useSearchConfig";
import useSearchOptions from "./hooks/useSearchOptions";
import useSelect from "./hooks/useSelect";
import useValues from "./hooks/useValues";
import OptionList from "./OptionList";
import Panel from "./Panel";
import { fillFieldNames, SHOW_CHILD, SHOW_PARENT, toPathKeys, toRawValues } from "./utils/commonUtil";
import { formatStrategyValues, toPathOptions } from "./utils/treeUtil";
import warningProps, { warningNullOptions } from "./utils/warningPropsUtil";
var Cascader = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var id = props.id,
    _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-cascader' : _props$prefixCls,
    fieldNames = props.fieldNames,
    defaultValue = props.defaultValue,
    value = props.value,
    changeOnSelect = props.changeOnSelect,
    onChange = props.onChange,
    displayRender = props.displayRender,
    checkable = props.checkable,
    _props$autoClearSearc = props.autoClearSearchValue,
    autoClearSearchValue = _props$autoClearSearc === void 0 ? true : _props$autoClearSearc,
    searchValue = props.searchValue,
    onSearch = props.onSearch,
    showSearch = props.showSearch,
    expandTrigger = props.expandTrigger,
    options = props.options,
    dropdownPrefixCls = props.dropdownPrefixCls,
    loadData = props.loadData,
    popupVisible = props.popupVisible,
    open = props.open,
    popupClassName = props.popupClassName,
    dropdownClassName = props.dropdownClassName,
    dropdownMenuColumnStyle = props.dropdownMenuColumnStyle,
    customDropdownStyle = props.dropdownStyle,
    popupPlacement = props.popupPlacement,
    placement = props.placement,
    onDropdownVisibleChange = props.onDropdownVisibleChange,
    onPopupVisibleChange = props.onPopupVisibleChange,
    _props$expandIcon = props.expandIcon,
    expandIcon = _props$expandIcon === void 0 ? '>' : _props$expandIcon,
    loadingIcon = props.loadingIcon,
    children = props.children,
    _props$dropdownMatchS = props.dropdownMatchSelectWidth,
    dropdownMatchSelectWidth = _props$dropdownMatchS === void 0 ? false : _props$dropdownMatchS,
    _props$showCheckedStr = props.showCheckedStrategy,
    showCheckedStrategy = _props$showCheckedStr === void 0 ? SHOW_PARENT : _props$showCheckedStr,
    optionRender = props.optionRender,
    restProps = _objectWithoutProperties(props, _excluded);
  var mergedId = useId(id);
  var multiple = !!checkable;

  // =========================== Values ===========================
  var _useMergedState = useMergedState(defaultValue, {
      value: value,
      postState: toRawValues
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    rawValues = _useMergedState2[0],
    setRawValues = _useMergedState2[1];

  // ========================= FieldNames =========================
  var mergedFieldNames = React.useMemo(function () {
    return fillFieldNames(fieldNames);
  }, /* eslint-disable react-hooks/exhaustive-deps */
  [JSON.stringify(fieldNames)]
  /* eslint-enable react-hooks/exhaustive-deps */);

  // =========================== Option ===========================
  var _useOptions = useOptions(mergedFieldNames, options),
    _useOptions2 = _slicedToArray(_useOptions, 3),
    mergedOptions = _useOptions2[0],
    getPathKeyEntities = _useOptions2[1],
    getValueByKeyPath = _useOptions2[2];

  // =========================== Search ===========================
  var _useMergedState3 = useMergedState('', {
      value: searchValue,
      postState: function postState(search) {
        return search || '';
      }
    }),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    mergedSearchValue = _useMergedState4[0],
    setSearchValue = _useMergedState4[1];
  var onInternalSearch = function onInternalSearch(searchText, info) {
    setSearchValue(searchText);
    if (info.source !== 'blur' && onSearch) {
      onSearch(searchText);
    }
  };
  var _useSearchConfig = useSearchConfig(showSearch),
    _useSearchConfig2 = _slicedToArray(_useSearchConfig, 2),
    mergedShowSearch = _useSearchConfig2[0],
    searchConfig = _useSearchConfig2[1];
  var searchOptions = useSearchOptions(mergedSearchValue, mergedOptions, mergedFieldNames, dropdownPrefixCls || prefixCls, searchConfig, changeOnSelect);

  // =========================== Values ===========================
  var getMissingValues = useMissingValues(mergedOptions, mergedFieldNames);

  // Fill `rawValues` with checked conduction values
  var _useValues = useValues(multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues),
    _useValues2 = _slicedToArray(_useValues, 3),
    checkedValues = _useValues2[0],
    halfCheckedValues = _useValues2[1],
    missingCheckedValues = _useValues2[2];
  var deDuplicatedValues = React.useMemo(function () {
    var checkedKeys = toPathKeys(checkedValues);
    var deduplicateKeys = formatStrategyValues(checkedKeys, getPathKeyEntities, showCheckedStrategy);
    return [].concat(_toConsumableArray(missingCheckedValues), _toConsumableArray(getValueByKeyPath(deduplicateKeys)));
  }, [checkedValues, getPathKeyEntities, getValueByKeyPath, missingCheckedValues, showCheckedStrategy]);
  var displayValues = useDisplayValues(deDuplicatedValues, mergedOptions, mergedFieldNames, multiple, displayRender);

  // =========================== Change ===========================
  var triggerChange = useEvent(function (nextValues) {
    setRawValues(nextValues);

    // Save perf if no need trigger event
    if (onChange) {
      var nextRawValues = toRawValues(nextValues);
      var valueOptions = nextRawValues.map(function (valueCells) {
        return toPathOptions(valueCells, mergedOptions, mergedFieldNames).map(function (valueOpt) {
          return valueOpt.option;
        });
      });
      var triggerValues = multiple ? nextRawValues : nextRawValues[0];
      var triggerOptions = multiple ? valueOptions : valueOptions[0];
      onChange(triggerValues, triggerOptions);
    }
  });

  // =========================== Select ===========================
  var handleSelection = useSelect(multiple, triggerChange, checkedValues, halfCheckedValues, missingCheckedValues, getPathKeyEntities, getValueByKeyPath, showCheckedStrategy);
  var onInternalSelect = useEvent(function (valuePath) {
    if (!multiple || autoClearSearchValue) {
      setSearchValue('');
    }
    handleSelection(valuePath);
  });

  // Display Value change logic
  var onDisplayValuesChange = function onDisplayValuesChange(_, info) {
    if (info.type === 'clear') {
      triggerChange([]);
      return;
    }

    // Cascader do not support `add` type. Only support `remove`
    var _ref = info.values[0],
      valueCells = _ref.valueCells;
    onInternalSelect(valueCells);
  };

  // ============================ Open ============================
  var mergedOpen = open !== undefined ? open : popupVisible;
  var mergedDropdownClassName = dropdownClassName || popupClassName;
  var mergedPlacement = placement || popupPlacement;
  var onInternalDropdownVisibleChange = function onInternalDropdownVisibleChange(nextVisible) {
    onDropdownVisibleChange === null || onDropdownVisibleChange === void 0 || onDropdownVisibleChange(nextVisible);
    onPopupVisibleChange === null || onPopupVisibleChange === void 0 || onPopupVisibleChange(nextVisible);
  };

  // ========================== Warning ===========================
  if (process.env.NODE_ENV !== 'production') {
    warningProps(props);
    warningNullOptions(mergedOptions, mergedFieldNames);
  }

  // ========================== Context ===========================
  var cascaderContext = React.useMemo(function () {
    return {
      options: mergedOptions,
      fieldNames: mergedFieldNames,
      values: checkedValues,
      halfValues: halfCheckedValues,
      changeOnSelect: changeOnSelect,
      onSelect: onInternalSelect,
      checkable: checkable,
      searchOptions: searchOptions,
      dropdownPrefixCls: dropdownPrefixCls,
      loadData: loadData,
      expandTrigger: expandTrigger,
      expandIcon: expandIcon,
      loadingIcon: loadingIcon,
      dropdownMenuColumnStyle: dropdownMenuColumnStyle,
      optionRender: optionRender
    };
  }, [mergedOptions, mergedFieldNames, checkedValues, halfCheckedValues, changeOnSelect, onInternalSelect, checkable, searchOptions, dropdownPrefixCls, loadData, expandTrigger, expandIcon, loadingIcon, dropdownMenuColumnStyle, optionRender]);

  // ==============================================================
  // ==                          Render                          ==
  // ==============================================================
  var emptyOptions = !(mergedSearchValue ? searchOptions : mergedOptions).length;
  var dropdownStyle =
  // Search to match width
  mergedSearchValue && searchConfig.matchInputWidth ||
  // Empty keep the width
  emptyOptions ? {} : {
    minWidth: 'auto'
  };
  return /*#__PURE__*/React.createElement(CascaderContext.Provider, {
    value: cascaderContext
  }, /*#__PURE__*/React.createElement(BaseSelect, _extends({}, restProps, {
    // MISC
    ref: ref,
    id: mergedId,
    prefixCls: prefixCls,
    autoClearSearchValue: autoClearSearchValue,
    dropdownMatchSelectWidth: dropdownMatchSelectWidth,
    dropdownStyle: _objectSpread(_objectSpread({}, dropdownStyle), customDropdownStyle)
    // Value
    ,
    displayValues: displayValues,
    onDisplayValuesChange: onDisplayValuesChange,
    mode: multiple ? 'multiple' : undefined
    // Search
    ,
    searchValue: mergedSearchValue,
    onSearch: onInternalSearch,
    showSearch: mergedShowSearch
    // Options
    ,
    OptionList: OptionList,
    emptyOptions: emptyOptions
    // Open
    ,
    open: mergedOpen,
    dropdownClassName: mergedDropdownClassName,
    placement: mergedPlacement,
    onDropdownVisibleChange: onInternalDropdownVisibleChange
    // Children
    ,
    getRawInputElement: function getRawInputElement() {
      return children;
    }
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Cascader.displayName = 'Cascader';
}
Cascader.SHOW_PARENT = SHOW_PARENT;
Cascader.SHOW_CHILD = SHOW_CHILD;
Cascader.Panel = Panel;
export default Cascader;