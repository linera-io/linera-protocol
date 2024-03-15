"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Panel;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcUtil = require("rc-util");
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("./context"));
var _useMissingValues = _interopRequireDefault(require("./hooks/useMissingValues"));
var _useOptions3 = _interopRequireDefault(require("./hooks/useOptions"));
var _useSelect = _interopRequireDefault(require("./hooks/useSelect"));
var _useValues3 = _interopRequireDefault(require("./hooks/useValues"));
var _List = _interopRequireDefault(require("./OptionList/List"));
var _commonUtil = require("./utils/commonUtil");
var _treeUtil = require("./utils/treeUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function noop() {}
function Panel(props) {
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
  var _useMergedState = (0, _rcUtil.useMergedState)(defaultValue, {
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

  // ========================= Values =========================
  var getMissingValues = (0, _useMissingValues.default)(mergedOptions, mergedFieldNames);

  // Fill `rawValues` with checked conduction values
  var _useValues = (0, _useValues3.default)(multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues),
    _useValues2 = (0, _slicedToArray2.default)(_useValues, 3),
    checkedValues = _useValues2[0],
    halfCheckedValues = _useValues2[1],
    missingCheckedValues = _useValues2[2];

  // =========================== Change ===========================
  var triggerChange = (0, _rcUtil.useEvent)(function (nextValues) {
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
  var onInternalSelect = (0, _rcUtil.useEvent)(function (valuePath) {
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
  return /*#__PURE__*/React.createElement(_context.default.Provider, {
    value: cascaderContext
  }, /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(panelPrefixCls, (_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(panelPrefixCls, "-rtl"), direction === 'rtl'), (0, _defineProperty2.default)(_classNames, "".concat(panelPrefixCls, "-empty"), isEmpty), _classNames), className),
    style: style
  }, isEmpty ? notFoundContent : /*#__PURE__*/React.createElement(_List.default, {
    prefixCls: prefixCls,
    searchValue: null,
    multiple: multiple,
    toggleOpen: noop,
    open: true,
    direction: direction
  })));
}