"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _rcSelect = require("rc-select");
var _useId = _interopRequireDefault(require("rc-select/lib/hooks/useId"));
var _useEvent = _interopRequireDefault(require("rc-util/lib/hooks/useEvent"));
var _useMergedState5 = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("./context"));
var _useDisplayValues = _interopRequireDefault(require("./hooks/useDisplayValues"));
var _useMissingValues = _interopRequireDefault(require("./hooks/useMissingValues"));
var _useOptions3 = _interopRequireDefault(require("./hooks/useOptions"));
var _useSearchConfig3 = _interopRequireDefault(require("./hooks/useSearchConfig"));
var _useSearchOptions = _interopRequireDefault(require("./hooks/useSearchOptions"));
var _useSelect = _interopRequireDefault(require("./hooks/useSelect"));
var _useValues3 = _interopRequireDefault(require("./hooks/useValues"));
var _OptionList = _interopRequireDefault(require("./OptionList"));
var _Panel = _interopRequireDefault(require("./Panel"));
var _commonUtil = require("./utils/commonUtil");
var _treeUtil = require("./utils/treeUtil");
var _warningPropsUtil = _interopRequireWildcard(require("./utils/warningPropsUtil"));
var _excluded = ["id", "prefixCls", "fieldNames", "defaultValue", "value", "changeOnSelect", "onChange", "displayRender", "checkable", "autoClearSearchValue", "searchValue", "onSearch", "showSearch", "expandTrigger", "options", "dropdownPrefixCls", "loadData", "popupVisible", "open", "popupClassName", "dropdownClassName", "dropdownMenuColumnStyle", "dropdownStyle", "popupPlacement", "placement", "onDropdownVisibleChange", "onPopupVisibleChange", "expandIcon", "loadingIcon", "children", "dropdownMatchSelectWidth", "showCheckedStrategy", "optionRender"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
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
    showCheckedStrategy = _props$showCheckedStr === void 0 ? _commonUtil.SHOW_PARENT : _props$showCheckedStr,
    optionRender = props.optionRender,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var mergedId = (0, _useId.default)(id);
  var multiple = !!checkable;

  // =========================== Values ===========================
  var _useMergedState = (0, _useMergedState5.default)(defaultValue, {
      value: value,
      postState: _commonUtil.toRawValues
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    rawValues = _useMergedState2[0],
    setRawValues = _useMergedState2[1];

  // ========================= FieldNames =========================
  var mergedFieldNames = React.useMemo(function () {
    return (0, _commonUtil.fillFieldNames)(fieldNames);
  }, /* eslint-disable react-hooks/exhaustive-deps */
  [JSON.stringify(fieldNames)]
  /* eslint-enable react-hooks/exhaustive-deps */);

  // =========================== Option ===========================
  var _useOptions = (0, _useOptions3.default)(mergedFieldNames, options),
    _useOptions2 = (0, _slicedToArray2.default)(_useOptions, 3),
    mergedOptions = _useOptions2[0],
    getPathKeyEntities = _useOptions2[1],
    getValueByKeyPath = _useOptions2[2];

  // =========================== Search ===========================
  var _useMergedState3 = (0, _useMergedState5.default)('', {
      value: searchValue,
      postState: function postState(search) {
        return search || '';
      }
    }),
    _useMergedState4 = (0, _slicedToArray2.default)(_useMergedState3, 2),
    mergedSearchValue = _useMergedState4[0],
    setSearchValue = _useMergedState4[1];
  var onInternalSearch = function onInternalSearch(searchText, info) {
    setSearchValue(searchText);
    if (info.source !== 'blur' && onSearch) {
      onSearch(searchText);
    }
  };
  var _useSearchConfig = (0, _useSearchConfig3.default)(showSearch),
    _useSearchConfig2 = (0, _slicedToArray2.default)(_useSearchConfig, 2),
    mergedShowSearch = _useSearchConfig2[0],
    searchConfig = _useSearchConfig2[1];
  var searchOptions = (0, _useSearchOptions.default)(mergedSearchValue, mergedOptions, mergedFieldNames, dropdownPrefixCls || prefixCls, searchConfig, changeOnSelect);

  // =========================== Values ===========================
  var getMissingValues = (0, _useMissingValues.default)(mergedOptions, mergedFieldNames);

  // Fill `rawValues` with checked conduction values
  var _useValues = (0, _useValues3.default)(multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues),
    _useValues2 = (0, _slicedToArray2.default)(_useValues, 3),
    checkedValues = _useValues2[0],
    halfCheckedValues = _useValues2[1],
    missingCheckedValues = _useValues2[2];
  var deDuplicatedValues = React.useMemo(function () {
    var checkedKeys = (0, _commonUtil.toPathKeys)(checkedValues);
    var deduplicateKeys = (0, _treeUtil.formatStrategyValues)(checkedKeys, getPathKeyEntities, showCheckedStrategy);
    return [].concat((0, _toConsumableArray2.default)(missingCheckedValues), (0, _toConsumableArray2.default)(getValueByKeyPath(deduplicateKeys)));
  }, [checkedValues, getPathKeyEntities, getValueByKeyPath, missingCheckedValues, showCheckedStrategy]);
  var displayValues = (0, _useDisplayValues.default)(deDuplicatedValues, mergedOptions, mergedFieldNames, multiple, displayRender);

  // =========================== Change ===========================
  var triggerChange = (0, _useEvent.default)(function (nextValues) {
    setRawValues(nextValues);

    // Save perf if no need trigger event
    if (onChange) {
      var nextRawValues = (0, _commonUtil.toRawValues)(nextValues);
      var valueOptions = nextRawValues.map(function (valueCells) {
        return (0, _treeUtil.toPathOptions)(valueCells, mergedOptions, mergedFieldNames).map(function (valueOpt) {
          return valueOpt.option;
        });
      });
      var triggerValues = multiple ? nextRawValues : nextRawValues[0];
      var triggerOptions = multiple ? valueOptions : valueOptions[0];
      onChange(triggerValues, triggerOptions);
    }
  });

  // =========================== Select ===========================
  var handleSelection = (0, _useSelect.default)(multiple, triggerChange, checkedValues, halfCheckedValues, missingCheckedValues, getPathKeyEntities, getValueByKeyPath, showCheckedStrategy);
  var onInternalSelect = (0, _useEvent.default)(function (valuePath) {
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
    (0, _warningPropsUtil.default)(props);
    (0, _warningPropsUtil.warningNullOptions)(mergedOptions, mergedFieldNames);
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
  return /*#__PURE__*/React.createElement(_context.default.Provider, {
    value: cascaderContext
  }, /*#__PURE__*/React.createElement(_rcSelect.BaseSelect, (0, _extends2.default)({}, restProps, {
    // MISC
    ref: ref,
    id: mergedId,
    prefixCls: prefixCls,
    autoClearSearchValue: autoClearSearchValue,
    dropdownMatchSelectWidth: dropdownMatchSelectWidth,
    dropdownStyle: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, dropdownStyle), customDropdownStyle)
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
    OptionList: _OptionList.default,
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
Cascader.SHOW_PARENT = _commonUtil.SHOW_PARENT;
Cascader.SHOW_CHILD = _commonUtil.SHOW_CHILD;
Cascader.Panel = _Panel.default;
var _default = exports.default = Cascader;