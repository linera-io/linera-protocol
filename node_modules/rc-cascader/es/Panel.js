import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import { useEvent, useMergedState } from 'rc-util';
import * as React from 'react';
import CascaderContext from "./context";
import useMissingValues from "./hooks/useMissingValues";
import useOptions from "./hooks/useOptions";
import useSelect from "./hooks/useSelect";
import useValues from "./hooks/useValues";
import RawOptionList from "./OptionList/List";
import { fillFieldNames, toRawValues } from "./utils/commonUtil";
import { toPathOptions } from "./utils/treeUtil";
function noop() {}
export default function Panel(props) {
  var _classNames;
  var _ref = props,
    _ref$prefixCls = _ref.prefixCls,
    prefixCls = _ref$prefixCls === void 0 ? 'rc-cascader' : _ref$prefixCls,
    style = _ref.style,
    className = _ref.className,
    options = _ref.options,
    checkable = _ref.checkable,
    defaultValue = _ref.defaultValue,
    value = _ref.value,
    fieldNames = _ref.fieldNames,
    changeOnSelect = _ref.changeOnSelect,
    onChange = _ref.onChange,
    showCheckedStrategy = _ref.showCheckedStrategy,
    loadData = _ref.loadData,
    expandTrigger = _ref.expandTrigger,
    _ref$expandIcon = _ref.expandIcon,
    expandIcon = _ref$expandIcon === void 0 ? '>' : _ref$expandIcon,
    loadingIcon = _ref.loadingIcon,
    direction = _ref.direction,
    _ref$notFoundContent = _ref.notFoundContent,
    notFoundContent = _ref$notFoundContent === void 0 ? 'Not Found' : _ref$notFoundContent;

  // ======================== Multiple ========================
  var multiple = !!checkable;

  // ========================= Values =========================
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

  // ========================= Values =========================
  var getMissingValues = useMissingValues(mergedOptions, mergedFieldNames);

  // Fill `rawValues` with checked conduction values
  var _useValues = useValues(multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues),
    _useValues2 = _slicedToArray(_useValues, 3),
    checkedValues = _useValues2[0],
    halfCheckedValues = _useValues2[1],
    missingCheckedValues = _useValues2[2];

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
    handleSelection(valuePath);
  });

  // ======================== Context =========================
  var cascaderContext = React.useMemo(function () {
    return {
      options: mergedOptions,
      fieldNames: mergedFieldNames,
      values: checkedValues,
      halfValues: halfCheckedValues,
      changeOnSelect: changeOnSelect,
      onSelect: onInternalSelect,
      checkable: checkable,
      searchOptions: [],
      dropdownPrefixCls: null,
      loadData: loadData,
      expandTrigger: expandTrigger,
      expandIcon: expandIcon,
      loadingIcon: loadingIcon,
      dropdownMenuColumnStyle: null
    };
  }, [mergedOptions, mergedFieldNames, checkedValues, halfCheckedValues, changeOnSelect, onInternalSelect, checkable, loadData, expandTrigger, expandIcon, loadingIcon]);

  // ========================= Render =========================
  var panelPrefixCls = "".concat(prefixCls, "-panel");
  var isEmpty = !mergedOptions.length;
  return /*#__PURE__*/React.createElement(CascaderContext.Provider, {
    value: cascaderContext
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames(panelPrefixCls, (_classNames = {}, _defineProperty(_classNames, "".concat(panelPrefixCls, "-rtl"), direction === 'rtl'), _defineProperty(_classNames, "".concat(panelPrefixCls, "-empty"), isEmpty), _classNames), className),
    style: style
  }, isEmpty ? notFoundContent : /*#__PURE__*/React.createElement(RawOptionList, {
    prefixCls: prefixCls,
    searchValue: null,
    multiple: multiple,
    toggleOpen: noop,
    open: true,
    direction: direction
  })));
}