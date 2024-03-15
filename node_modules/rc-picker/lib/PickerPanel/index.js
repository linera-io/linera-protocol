"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcUtil = require("rc-util");
var React = _interopRequireWildcard(require("react"));
var _useLocale = _interopRequireDefault(require("../hooks/useLocale"));
var _useTimeConfig = require("../hooks/useTimeConfig");
var _useToggleDates = _interopRequireDefault(require("../hooks/useToggleDates"));
var _context = _interopRequireDefault(require("../PickerInput/context"));
var _useCellRender = _interopRequireDefault(require("../PickerInput/hooks/useCellRender"));
var _dateUtil = require("../utils/dateUtil");
var _miscUtil = require("../utils/miscUtil");
var _context2 = require("./context");
var _DatePanel = _interopRequireDefault(require("./DatePanel"));
var _DateTimePanel = _interopRequireDefault(require("./DateTimePanel"));
var _DecadePanel = _interopRequireDefault(require("./DecadePanel"));
var _MonthPanel = _interopRequireDefault(require("./MonthPanel"));
var _QuarterPanel = _interopRequireDefault(require("./QuarterPanel"));
var _TimePanel = _interopRequireDefault(require("./TimePanel"));
var _WeekPanel = _interopRequireDefault(require("./WeekPanel"));
var _YearPanel = _interopRequireDefault(require("./YearPanel"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var DefaultComponents = {
  date: _DatePanel.default,
  datetime: _DateTimePanel.default,
  week: _WeekPanel.default,
  month: _MonthPanel.default,
  quarter: _QuarterPanel.default,
  year: _YearPanel.default,
  decade: _DecadePanel.default,
  time: _TimePanel.default
};
function PickerPanel(props, ref) {
  var _React$useContext;
  var locale = props.locale,
    generateConfig = props.generateConfig,
    direction = props.direction,
    prefixCls = props.prefixCls,
    _props$tabIndex = props.tabIndex,
    tabIndex = _props$tabIndex === void 0 ? 0 : _props$tabIndex,
    multiple = props.multiple,
    defaultValue = props.defaultValue,
    value = props.value,
    onChange = props.onChange,
    onSelect = props.onSelect,
    defaultPickerValue = props.defaultPickerValue,
    pickerValue = props.pickerValue,
    onPickerValueChange = props.onPickerValueChange,
    mode = props.mode,
    onPanelChange = props.onPanelChange,
    _props$picker = props.picker,
    picker = _props$picker === void 0 ? 'date' : _props$picker,
    showTime = props.showTime,
    hoverValue = props.hoverValue,
    hoverRangeValue = props.hoverRangeValue,
    cellRender = props.cellRender,
    dateRender = props.dateRender,
    monthCellRender = props.monthCellRender,
    _props$components = props.components,
    components = _props$components === void 0 ? {} : _props$components,
    hideHeader = props.hideHeader;
  var mergedPrefixCls = ((_React$useContext = React.useContext(_context.default)) === null || _React$useContext === void 0 ? void 0 : _React$useContext.prefixCls) || prefixCls || 'rc-picker';

  // ========================== Refs ==========================
  var rootRef = React.useRef();
  React.useImperativeHandle(ref, function () {
    return {
      nativeElement: rootRef.current
    };
  });

  // ========================== Time ==========================
  // Auto `format` need to check `showTime.showXXX` first.
  // And then merge the `locale` into `mergedShowTime`.
  var _getTimeProps = (0, _useTimeConfig.getTimeProps)(props),
    _getTimeProps2 = (0, _slicedToArray2.default)(_getTimeProps, 4),
    timeProps = _getTimeProps2[0],
    localeTimeProps = _getTimeProps2[1],
    showTimeFormat = _getTimeProps2[2],
    propFormat = _getTimeProps2[3];

  // ========================= Locale =========================
  var filledLocale = (0, _useLocale.default)(locale, localeTimeProps);

  // ========================= Picker =========================
  var internalPicker = picker === 'date' && showTime ? 'datetime' : picker;

  // ======================== ShowTime ========================
  var mergedShowTime = React.useMemo(function () {
    return (0, _useTimeConfig.fillShowTimeConfig)(internalPicker, showTimeFormat, propFormat, timeProps, filledLocale);
  }, [internalPicker, showTimeFormat, propFormat, timeProps, filledLocale]);

  // ========================== Now ===========================
  var now = generateConfig.getNow();

  // ========================== Mode ==========================
  var _useMergedState = (0, _rcUtil.useMergedState)(picker, {
      value: mode,
      postState: function postState(val) {
        return val || 'date';
      }
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    mergedMode = _useMergedState2[0],
    setMergedMode = _useMergedState2[1];
  var internalMode = mergedMode === 'date' && mergedShowTime ? 'datetime' : mergedMode;

  // ========================= Toggle =========================
  var toggleDates = (0, _useToggleDates.default)(generateConfig, locale, internalPicker);

  // ========================= Value ==========================
  // >>> Real value
  // Interactive with `onChange` event which only trigger when the `mode` is `picker`
  var _useMergedState3 = (0, _rcUtil.useMergedState)(defaultValue, {
      value: value
    }),
    _useMergedState4 = (0, _slicedToArray2.default)(_useMergedState3, 2),
    innerValue = _useMergedState4[0],
    setMergedValue = _useMergedState4[1];
  var mergedValue = React.useMemo(function () {
    // Clean up `[null]`
    var values = (0, _miscUtil.toArray)(innerValue).filter(function (val) {
      return val;
    });
    return multiple ? values : values.slice(0, 1);
  }, [innerValue, multiple]);

  // Sync value and only trigger onChange event when changed
  var triggerChange = (0, _rcUtil.useEvent)(function (nextValue) {
    setMergedValue(nextValue);
    if (onChange && (nextValue === null || mergedValue.length !== nextValue.length || mergedValue.some(function (ori, index) {
      return !(0, _dateUtil.isSame)(generateConfig, locale, ori, nextValue[index], internalPicker);
    }))) {
      onChange === null || onChange === void 0 || onChange(multiple ? nextValue : nextValue[0]);
    }
  });

  // >>> CalendarValue
  // CalendarValue is a temp value for user operation
  // which will only trigger `onCalendarChange` but not `onChange`
  var onInternalSelect = (0, _rcUtil.useEvent)(function (newDate) {
    onSelect === null || onSelect === void 0 || onSelect(newDate);
    if (mergedMode === picker) {
      var nextValues = multiple ? toggleDates(mergedValue, newDate) : [newDate];
      triggerChange(nextValues);
    }
  });

  // >>> PickerValue
  // PickerValue is used to control the current displaying panel
  var _useMergedState5 = (0, _rcUtil.useMergedState)(defaultPickerValue || mergedValue[0] || now, {
      value: pickerValue
    }),
    _useMergedState6 = (0, _slicedToArray2.default)(_useMergedState5, 2),
    mergedPickerValue = _useMergedState6[0],
    setInternalPickerValue = _useMergedState6[1];
  React.useEffect(function () {
    if (mergedValue[0] && !pickerValue) {
      setInternalPickerValue(mergedValue[0]);
    }
  }, [mergedValue[0]]);

  // Both trigger when manually pickerValue or mode change
  var triggerPanelChange = function triggerPanelChange(viewDate, nextMode) {
    onPanelChange === null || onPanelChange === void 0 || onPanelChange(viewDate || pickerValue, nextMode || mergedMode);
  };
  var setPickerValue = function setPickerValue(nextPickerValue) {
    var triggerPanelEvent = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
    setInternalPickerValue(nextPickerValue);
    onPickerValueChange === null || onPickerValueChange === void 0 || onPickerValueChange(nextPickerValue);
    if (triggerPanelEvent) {
      triggerPanelChange(nextPickerValue);
    }
  };
  var triggerModeChange = function triggerModeChange(nextMode, viewDate) {
    setMergedMode(nextMode);
    if (viewDate) {
      setPickerValue(viewDate);
    }
    triggerPanelChange(viewDate, nextMode);
  };
  var onPanelValueSelect = function onPanelValueSelect(nextValue) {
    onInternalSelect(nextValue);
    setPickerValue(nextValue);

    // Update mode if needed
    if (mergedMode !== picker) {
      var decadeYearQueue = ['decade', 'year'];
      var decadeYearMonthQueue = [].concat(decadeYearQueue, ['month']);
      var pickerQueue = {
        quarter: [].concat(decadeYearQueue, ['quarter']),
        week: [].concat((0, _toConsumableArray2.default)(decadeYearMonthQueue), ['week']),
        date: [].concat((0, _toConsumableArray2.default)(decadeYearMonthQueue), ['date'])
      };
      var queue = pickerQueue[picker] || decadeYearMonthQueue;
      var index = queue.indexOf(mergedMode);
      var nextMode = queue[index + 1];
      if (nextMode) {
        triggerModeChange(nextMode, nextValue);
      }
    }
  };

  // ======================= Hover Date =======================
  var hoverRangeDate = React.useMemo(function () {
    var start;
    var end;
    if (Array.isArray(hoverRangeValue)) {
      var _hoverRangeValue = (0, _slicedToArray2.default)(hoverRangeValue, 2);
      start = _hoverRangeValue[0];
      end = _hoverRangeValue[1];
    } else {
      start = hoverRangeValue;
    }

    // Return for not exist
    if (!start && !end) {
      return null;
    }

    // Fill if has empty
    start = start || end;
    end = end || start;
    return generateConfig.isAfter(start, end) ? [end, start] : [start, end];
  }, [hoverRangeValue, generateConfig]);

  // ======================= Components =======================
  // >>> cellRender
  var onInternalCellRender = (0, _useCellRender.default)(cellRender, dateRender, monthCellRender);

  // ======================= Components =======================
  var PanelComponent = components[internalMode] || DefaultComponents[internalMode] || _DatePanel.default;

  // ======================== Context =========================
  var parentHackContext = React.useContext(_context2.PickerHackContext);
  var pickerPanelContext = React.useMemo(function () {
    return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, parentHackContext), {}, {
      hideHeader: hideHeader
    });
  }, [parentHackContext, hideHeader]);

  // ======================== Warnings ========================
  if (process.env.NODE_ENV !== 'production') {
    (0, _rcUtil.warning)(!mergedValue || mergedValue.every(function (val) {
      return generateConfig.isValidate(val);
    }), 'Invalidate date pass to `value` or `defaultValue`.');
  }

  // ========================= Render =========================
  var panelCls = "".concat(mergedPrefixCls, "-panel");
  var panelProps = (0, _miscUtil.pickProps)(props, [
  // Week
  'showWeek',
  // Icons
  'prevIcon', 'nextIcon', 'superPrevIcon', 'superNextIcon',
  // Disabled
  'disabledDate', 'minDate', 'maxDate',
  // Hover
  'onHover']);
  return /*#__PURE__*/React.createElement(_context2.PickerHackContext.Provider, {
    value: pickerPanelContext
  }, /*#__PURE__*/React.createElement("div", {
    ref: rootRef,
    tabIndex: tabIndex,
    className: (0, _classnames.default)(panelCls, (0, _defineProperty2.default)({}, "".concat(panelCls, "-rtl"), direction === 'rtl'))
  }, /*#__PURE__*/React.createElement(PanelComponent, (0, _extends2.default)({}, panelProps, {
    // Time
    showTime: mergedShowTime
    // MISC
    ,
    prefixCls: mergedPrefixCls,
    locale: filledLocale,
    generateConfig: generateConfig
    // Mode
    ,
    onModeChange: triggerModeChange
    // Value
    ,
    pickerValue: mergedPickerValue,
    onPickerValueChange: function onPickerValueChange(nextPickerValue) {
      setPickerValue(nextPickerValue, true);
    },
    value: mergedValue[0],
    onSelect: onPanelValueSelect,
    values: mergedValue
    // Render
    ,
    cellRender: onInternalCellRender
    // Hover
    ,
    hoverRangeValue: hoverRangeDate,
    hoverValue: hoverValue
  }))));
}
var RefPanelPicker = /*#__PURE__*/React.memo( /*#__PURE__*/React.forwardRef(PickerPanel));
if (process.env.NODE_ENV !== 'production') {
  RefPanelPicker.displayName = 'PanelPicker';
}

// Make support generic
var _default = exports.default = RefPanelPicker;