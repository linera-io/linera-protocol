"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _rcUtil = require("rc-util");
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var React = _interopRequireWildcard(require("react"));
var _useToggleDates = _interopRequireDefault(require("../hooks/useToggleDates"));
var _PickerTrigger = _interopRequireDefault(require("../PickerTrigger"));
var _util = require("../PickerTrigger/util");
var _miscUtil = require("../utils/miscUtil");
var _context = _interopRequireDefault(require("./context"));
var _useCellRender = _interopRequireDefault(require("./hooks/useCellRender"));
var _useFieldsInvalidate3 = _interopRequireDefault(require("./hooks/useFieldsInvalidate"));
var _useFilledProps3 = _interopRequireDefault(require("./hooks/useFilledProps"));
var _useOpen3 = _interopRequireDefault(require("./hooks/useOpen"));
var _usePickerRef = _interopRequireDefault(require("./hooks/usePickerRef"));
var _usePresets = _interopRequireDefault(require("./hooks/usePresets"));
var _useRangeActive3 = _interopRequireDefault(require("./hooks/useRangeActive"));
var _useRangePickerValue3 = _interopRequireDefault(require("./hooks/useRangePickerValue"));
var _useRangeValue3 = _interopRequireWildcard(require("./hooks/useRangeValue"));
var _useShowNow = _interopRequireDefault(require("./hooks/useShowNow"));
var _Popup = _interopRequireDefault(require("./Popup"));
var _SingleSelector = _interopRequireDefault(require("./Selector/SingleSelector"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
// TODO: isInvalidateDate with showTime.disabledTime should not provide `range` prop

/** Internal usage. For cross function get same aligned props */

function Picker(props, ref) {
  // ========================= Prop =========================
  var _useFilledProps = (0, _useFilledProps3.default)(props),
    _useFilledProps2 = (0, _slicedToArray2.default)(_useFilledProps, 6),
    filledProps = _useFilledProps2[0],
    internalPicker = _useFilledProps2[1],
    complexPicker = _useFilledProps2[2],
    formatList = _useFilledProps2[3],
    maskFormat = _useFilledProps2[4],
    isInvalidateDate = _useFilledProps2[5];
  var _ref = filledProps,
    prefixCls = _ref.prefixCls,
    styles = _ref.styles,
    classNames = _ref.classNames,
    order = _ref.order,
    defaultValue = _ref.defaultValue,
    value = _ref.value,
    needConfirm = _ref.needConfirm,
    onChange = _ref.onChange,
    onKeyDown = _ref.onKeyDown,
    disabled = _ref.disabled,
    disabledDate = _ref.disabledDate,
    minDate = _ref.minDate,
    maxDate = _ref.maxDate,
    defaultOpen = _ref.defaultOpen,
    open = _ref.open,
    onOpenChange = _ref.onOpenChange,
    locale = _ref.locale,
    generateConfig = _ref.generateConfig,
    picker = _ref.picker,
    showNow = _ref.showNow,
    showToday = _ref.showToday,
    showTime = _ref.showTime,
    mode = _ref.mode,
    onPanelChange = _ref.onPanelChange,
    onCalendarChange = _ref.onCalendarChange,
    onOk = _ref.onOk,
    multiple = _ref.multiple,
    defaultPickerValue = _ref.defaultPickerValue,
    pickerValue = _ref.pickerValue,
    onPickerValueChange = _ref.onPickerValueChange,
    inputReadOnly = _ref.inputReadOnly,
    suffixIcon = _ref.suffixIcon,
    removeIcon = _ref.removeIcon,
    onFocus = _ref.onFocus,
    onBlur = _ref.onBlur,
    presets = _ref.presets,
    components = _ref.components,
    cellRender = _ref.cellRender,
    dateRender = _ref.dateRender,
    monthCellRender = _ref.monthCellRender,
    onClick = _ref.onClick;

  // ========================= Refs =========================
  var selectorRef = (0, _usePickerRef.default)(ref);

  // ========================= Util =========================
  function pickerParam(values) {
    if (values === null) {
      return null;
    }
    return multiple ? values : values[0];
  }
  var toggleDates = (0, _useToggleDates.default)(generateConfig, locale, internalPicker);

  // ========================= Open =========================
  var _useOpen = (0, _useOpen3.default)(open, defaultOpen, [disabled], onOpenChange),
    _useOpen2 = (0, _slicedToArray2.default)(_useOpen, 2),
    mergedOpen = _useOpen2[0],
    triggerOpen = _useOpen2[1];

  // ======================= Calendar =======================
  var onInternalCalendarChange = function onInternalCalendarChange(dates, dateStrings, info) {
    if (onCalendarChange) {
      var filteredInfo = (0, _objectSpread2.default)({}, info);
      delete filteredInfo.range;
      onCalendarChange(pickerParam(dates), pickerParam(dateStrings), filteredInfo);
    }
  };
  var onInternalOk = function onInternalOk(dates) {
    onOk === null || onOk === void 0 || onOk(pickerParam(dates));
  };

  // ======================== Values ========================
  var _useInnerValue = (0, _useRangeValue3.useInnerValue)(generateConfig, locale, formatList, false, order, defaultValue, value, onInternalCalendarChange, onInternalOk),
    _useInnerValue2 = (0, _slicedToArray2.default)(_useInnerValue, 5),
    mergedValue = _useInnerValue2[0],
    setInnerValue = _useInnerValue2[1],
    getCalendarValue = _useInnerValue2[2],
    triggerCalendarChange = _useInnerValue2[3],
    triggerOk = _useInnerValue2[4];
  var calendarValue = getCalendarValue();

  // ======================== Active ========================
  // In SinglePicker, we will always get `activeIndex` is 0.
  var _useRangeActive = (0, _useRangeActive3.default)([disabled]),
    _useRangeActive2 = (0, _slicedToArray2.default)(_useRangeActive, 4),
    focused = _useRangeActive2[0],
    triggerFocus = _useRangeActive2[1],
    lastOperation = _useRangeActive2[2],
    activeIndex = _useRangeActive2[3];
  var onSharedFocus = function onSharedFocus(event) {
    triggerFocus(true);
    onFocus === null || onFocus === void 0 || onFocus(event, {});
  };
  var onSharedBlur = function onSharedBlur(event) {
    triggerFocus(false);
    onBlur === null || onBlur === void 0 || onBlur(event, {});
  };

  // ========================= Mode =========================
  var _useMergedState = (0, _rcUtil.useMergedState)(picker, {
      value: mode
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    mergedMode = _useMergedState2[0],
    setMode = _useMergedState2[1];

  /** Extends from `mergedMode` to patch `datetime` mode */
  var internalMode = mergedMode === 'date' && showTime ? 'datetime' : mergedMode;

  // ======================= Show Now =======================
  var mergedShowNow = (0, _useShowNow.default)(picker, mergedMode, showNow, showToday);

  // ======================== Value =========================
  var onInternalChange = onChange && function (dates, dateStrings) {
    onChange(pickerParam(dates), pickerParam(dateStrings));
  };
  var _useRangeValue = (0, _useRangeValue3.default)((0, _objectSpread2.default)((0, _objectSpread2.default)({}, filledProps), {}, {
      onChange: onInternalChange
    }), mergedValue, setInnerValue, getCalendarValue, triggerCalendarChange, [],
    //disabled,
    formatList, focused, mergedOpen, isInvalidateDate),
    _useRangeValue2 = (0, _slicedToArray2.default)(_useRangeValue, 2),
    /** Trigger `onChange` directly without check `disabledDate` */
    triggerSubmitChange = _useRangeValue2[1];

  // ======================= Validate =======================
  var _useFieldsInvalidate = (0, _useFieldsInvalidate3.default)(calendarValue, isInvalidateDate),
    _useFieldsInvalidate2 = (0, _slicedToArray2.default)(_useFieldsInvalidate, 2),
    submitInvalidates = _useFieldsInvalidate2[0],
    onSelectorInvalid = _useFieldsInvalidate2[1];
  var submitInvalidate = React.useMemo(function () {
    return submitInvalidates.some(function (invalidated) {
      return invalidated;
    });
  }, [submitInvalidates]);

  // ===================== Picker Value =====================
  // Proxy to single pickerValue
  var onInternalPickerValueChange = function onInternalPickerValueChange(dates, info) {
    if (onPickerValueChange) {
      var cleanInfo = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, info), {}, {
        mode: info.mode[0]
      });
      delete cleanInfo.range;
      onPickerValueChange(dates[0], cleanInfo);
    }
  };
  var _useRangePickerValue = (0, _useRangePickerValue3.default)(generateConfig, locale, calendarValue, [mergedMode], mergedOpen, activeIndex, internalPicker, false,
    // multiplePanel,
    defaultPickerValue, pickerValue, (0, _miscUtil.toArray)(showTime === null || showTime === void 0 ? void 0 : showTime.defaultOpenValue), onInternalPickerValueChange, minDate, maxDate),
    _useRangePickerValue2 = (0, _slicedToArray2.default)(_useRangePickerValue, 2),
    currentPickerValue = _useRangePickerValue2[0],
    setCurrentPickerValue = _useRangePickerValue2[1];

  // >>> Mode need wait for `pickerValue`
  var triggerModeChange = (0, _rcUtil.useEvent)(function (nextPickerValue, nextMode, triggerEvent) {
    setMode(nextMode);

    // Compatible with `onPanelChange`
    if (onPanelChange && triggerEvent !== false) {
      var lastPickerValue = nextPickerValue || calendarValue[calendarValue.length - 1];
      onPanelChange(lastPickerValue, nextMode);
    }
  });

  // ======================== Submit ========================
  /**
   * Different with RangePicker, confirm should check `multiple` logic.
   * This will never provide `date` instead.
   */
  var triggerConfirm = function triggerConfirm() {
    triggerSubmitChange(getCalendarValue());
    triggerOpen(false, {
      force: true
    });
  };

  // ======================== Click =========================
  var onSelectorClick = function onSelectorClick(event) {
    if (!disabled && !selectorRef.current.nativeElement.contains(document.activeElement)) {
      // Click to focus the enabled input
      selectorRef.current.focus();
    }
    triggerOpen(true);
    onClick === null || onClick === void 0 || onClick(event);
  };
  var onSelectorClear = function onSelectorClear() {
    triggerSubmitChange(null);
    triggerOpen(false, {
      force: true
    });
  };

  // ======================== Hover =========================
  var _React$useState = React.useState(null),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    hoverSource = _React$useState2[0],
    setHoverSource = _React$useState2[1];
  var _React$useState3 = React.useState(null),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    internalHoverValue = _React$useState4[0],
    setInternalHoverValue = _React$useState4[1];
  var hoverValues = React.useMemo(function () {
    var values = [internalHoverValue].concat((0, _toConsumableArray2.default)(calendarValue)).filter(function (date) {
      return date;
    });
    return multiple ? values : values.slice(0, 1);
  }, [calendarValue, internalHoverValue, multiple]);

  // Selector values is different with RangePicker
  // which can not use `hoverValue` directly
  var selectorValues = React.useMemo(function () {
    if (!multiple && internalHoverValue) {
      return [internalHoverValue];
    }
    return calendarValue.filter(function (date) {
      return date;
    });
  }, [calendarValue, internalHoverValue, multiple]);

  // Clean up `internalHoverValues` when closed
  React.useEffect(function () {
    if (!mergedOpen) {
      setInternalHoverValue(null);
    }
  }, [mergedOpen]);

  // ========================================================
  // ==                       Panels                       ==
  // ========================================================
  // ======================= Presets ========================
  var presetList = (0, _usePresets.default)(presets);
  var onPresetHover = function onPresetHover(nextValue) {
    setInternalHoverValue(nextValue);
    setHoverSource('preset');
  };

  // TODO: handle this
  var onPresetSubmit = function onPresetSubmit(nextValue) {
    var nextCalendarValues = multiple ? toggleDates(getCalendarValue(), nextValue) : [nextValue];
    var passed = triggerSubmitChange(nextCalendarValues);
    if (passed && !multiple) {
      triggerOpen(false, {
        force: true
      });
    }
  };
  var onNow = function onNow(now) {
    onPresetSubmit(now);
  };

  // ======================== Panel =========================
  var onPanelHover = function onPanelHover(date) {
    setInternalHoverValue(date);
    setHoverSource('cell');
  };

  // >>> Focus
  var onPanelFocus = function onPanelFocus(event) {
    triggerOpen(true);
    onSharedFocus(event);
  };

  // >>> Calendar
  var onPanelSelect = function onPanelSelect(date) {
    lastOperation('panel');
    var nextValues = multiple ? toggleDates(getCalendarValue(), date) : [date];

    // Only trigger calendar event but not update internal `calendarValue` state
    triggerCalendarChange(nextValues);

    // >>> Trigger next active if !needConfirm
    // Fully logic check `useRangeValue` hook
    if (!needConfirm && !complexPicker && internalPicker === internalMode) {
      triggerConfirm();
    }
  };

  // >>> Close
  var onPopupClose = function onPopupClose() {
    // Close popup
    triggerOpen(false);
  };

  // >>> cellRender
  var onInternalCellRender = (0, _useCellRender.default)(cellRender, dateRender, monthCellRender);

  // >>> invalid

  var panelProps = React.useMemo(function () {
    var domProps = (0, _pickAttrs.default)(filledProps, false);
    var restProps = (0, _omit.default)(filledProps, [].concat((0, _toConsumableArray2.default)(Object.keys(domProps)), ['onChange', 'onCalendarChange', 'style', 'className', 'onPanelChange']));
    return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, restProps), {}, {
      multiple: filledProps.multiple
    });
  }, [filledProps]);

  // >>> Render
  var panel = /*#__PURE__*/React.createElement(_Popup.default, (0, _extends2.default)({}, panelProps, {
    showNow: mergedShowNow,
    showTime: showTime
    // Disabled
    ,
    disabledDate: disabledDate
    // Focus
    ,
    onFocus: onPanelFocus,
    onBlur: onSharedBlur
    // Mode
    ,
    picker: picker,
    mode: mergedMode,
    internalMode: internalMode,
    onPanelChange: triggerModeChange
    // Value
    ,
    format: maskFormat,
    value: calendarValue,
    isInvalid: isInvalidateDate,
    onChange: null,
    onSelect: onPanelSelect
    // PickerValue
    ,
    pickerValue: currentPickerValue,
    defaultOpenValue: showTime === null || showTime === void 0 ? void 0 : showTime.defaultOpenValue,
    onPickerValueChange: setCurrentPickerValue
    // Hover
    ,
    hoverValue: hoverValues,
    onHover: onPanelHover
    // Submit
    ,
    needConfirm: needConfirm,
    onSubmit: triggerConfirm,
    onOk: triggerOk
    // Preset
    ,
    presets: presetList,
    onPresetHover: onPresetHover,
    onPresetSubmit: onPresetSubmit,
    onNow: onNow
    // Render
    ,
    cellRender: onInternalCellRender
  }));

  // ========================================================
  // ==                      Selector                      ==
  // ========================================================

  // ======================== Change ========================
  var onSelectorChange = function onSelectorChange(date) {
    triggerCalendarChange(date);
  };
  var onSelectorInputChange = function onSelectorInputChange() {
    lastOperation('input');
  };

  // ======================= Selector =======================
  var onSelectorFocus = function onSelectorFocus(event) {
    lastOperation('input');
    triggerOpen(true, {
      inherit: true
    });

    // setActiveIndex(index);

    onSharedFocus(event);
  };
  var onSelectorBlur = function onSelectorBlur(event) {
    triggerOpen(false);
    onSharedBlur(event);
  };
  var onSelectorKeyDown = function onSelectorKeyDown(event, preventDefault) {
    if (event.key === 'Tab') {
      triggerConfirm();
    }
    onKeyDown === null || onKeyDown === void 0 || onKeyDown(event, preventDefault);
  };

  // ======================= Context ========================
  var context = React.useMemo(function () {
    return {
      prefixCls: prefixCls,
      locale: locale,
      generateConfig: generateConfig,
      button: components.button,
      input: components.input
    };
  }, [prefixCls, locale, generateConfig, components.button, components.input]);

  // ======================== Effect ========================
  // >>> Mode
  // Reset for every active
  (0, _useLayoutEffect.default)(function () {
    if (mergedOpen && activeIndex !== undefined) {
      // Legacy compatible. This effect update should not trigger `onPanelChange`
      triggerModeChange(null, picker, false);
    }
  }, [mergedOpen, activeIndex, picker]);

  // >>> For complex picker, we need check if need to focus next one
  (0, _useLayoutEffect.default)(function () {
    var lastOp = lastOperation();

    // Trade as confirm on field leave
    if (!mergedOpen && lastOp === 'input') {
      triggerOpen(false);
      triggerConfirm();
    }

    // Submit with complex picker
    if (!mergedOpen && complexPicker && !needConfirm && lastOp === 'panel') {
      triggerOpen(true);
      triggerConfirm();
    }
  }, [mergedOpen]);

  // ======================== Render ========================
  return /*#__PURE__*/React.createElement(_context.default.Provider, {
    value: context
  }, /*#__PURE__*/React.createElement(_PickerTrigger.default, (0, _extends2.default)({}, (0, _util.pickTriggerProps)(filledProps), {
    popupElement: panel,
    popupStyle: styles.popup,
    popupClassName: classNames.popup
    // Visible
    ,
    visible: mergedOpen,
    onClose: onPopupClose
  }), /*#__PURE__*/React.createElement(_SingleSelector.default
  // Shared
  , (0, _extends2.default)({}, filledProps, {
    // Ref
    ref: selectorRef
    // Icon
    ,
    suffixIcon: suffixIcon,
    removeIcon: removeIcon
    // Active
    ,
    activeHelp: !!internalHoverValue,
    allHelp: !!internalHoverValue && hoverSource === 'preset',
    focused: focused,
    onFocus: onSelectorFocus,
    onBlur: onSelectorBlur,
    onKeyDown: onSelectorKeyDown,
    onSubmit: triggerConfirm
    // Change
    ,
    value: selectorValues,
    maskFormat: maskFormat,
    onChange: onSelectorChange,
    onInputChange: onSelectorInputChange,
    internalPicker: internalPicker
    // Format
    ,
    format: formatList,
    inputReadOnly: inputReadOnly
    // Disabled
    ,
    disabled: disabled
    // Open
    ,
    open: mergedOpen,
    onOpenChange: triggerOpen
    // Click
    ,
    onClick: onSelectorClick,
    onClear: onSelectorClear
    // Invalid
    ,
    invalid: submitInvalidate,
    onInvalid: function onInvalid(invalid) {
      // Only `single` mode support type date.
      // `multiple` mode can not typing.
      onSelectorInvalid(invalid, 0);
    }
  }))));
}
var RefPicker = /*#__PURE__*/React.forwardRef(Picker);
if (process.env.NODE_ENV !== 'production') {
  RefPicker.displayName = 'RefPicker';
}
var _default = exports.default = RefPicker;