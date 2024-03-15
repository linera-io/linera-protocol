import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import { useEvent, useMergedState, warning } from 'rc-util';
import * as React from 'react';
import useLocale from "../hooks/useLocale";
import { fillShowTimeConfig, getTimeProps } from "../hooks/useTimeConfig";
import useToggleDates from "../hooks/useToggleDates";
import PickerContext from "../PickerInput/context";
import useCellRender from "../PickerInput/hooks/useCellRender";
import { isSame } from "../utils/dateUtil";
import { pickProps, toArray } from "../utils/miscUtil";
import { PickerHackContext } from "./context";
import DatePanel from "./DatePanel";
import DateTimePanel from "./DateTimePanel";
import DecadePanel from "./DecadePanel";
import MonthPanel from "./MonthPanel";
import QuarterPanel from "./QuarterPanel";
import TimePanel from "./TimePanel";
import WeekPanel from "./WeekPanel";
import YearPanel from "./YearPanel";
var DefaultComponents = {
  date: DatePanel,
  datetime: DateTimePanel,
  week: WeekPanel,
  month: MonthPanel,
  quarter: QuarterPanel,
  year: YearPanel,
  decade: DecadePanel,
  time: TimePanel
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
  var mergedPrefixCls = ((_React$useContext = React.useContext(PickerContext)) === null || _React$useContext === void 0 ? void 0 : _React$useContext.prefixCls) || prefixCls || 'rc-picker';

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
  var _getTimeProps = getTimeProps(props),
    _getTimeProps2 = _slicedToArray(_getTimeProps, 4),
    timeProps = _getTimeProps2[0],
    localeTimeProps = _getTimeProps2[1],
    showTimeFormat = _getTimeProps2[2],
    propFormat = _getTimeProps2[3];

  // ========================= Locale =========================
  var filledLocale = useLocale(locale, localeTimeProps);

  // ========================= Picker =========================
  var internalPicker = picker === 'date' && showTime ? 'datetime' : picker;

  // ======================== ShowTime ========================
  var mergedShowTime = React.useMemo(function () {
    return fillShowTimeConfig(internalPicker, showTimeFormat, propFormat, timeProps, filledLocale);
  }, [internalPicker, showTimeFormat, propFormat, timeProps, filledLocale]);

  // ========================== Now ===========================
  var now = generateConfig.getNow();

  // ========================== Mode ==========================
  var _useMergedState = useMergedState(picker, {
      value: mode,
      postState: function postState(val) {
        return val || 'date';
      }
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    mergedMode = _useMergedState2[0],
    setMergedMode = _useMergedState2[1];
  var internalMode = mergedMode === 'date' && mergedShowTime ? 'datetime' : mergedMode;

  // ========================= Toggle =========================
  var toggleDates = useToggleDates(generateConfig, locale, internalPicker);

  // ========================= Value ==========================
  // >>> Real value
  // Interactive with `onChange` event which only trigger when the `mode` is `picker`
  var _useMergedState3 = useMergedState(defaultValue, {
      value: value
    }),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    innerValue = _useMergedState4[0],
    setMergedValue = _useMergedState4[1];
  var mergedValue = React.useMemo(function () {
    // Clean up `[null]`
    var values = toArray(innerValue).filter(function (val) {
      return val;
    });
    return multiple ? values : values.slice(0, 1);
  }, [innerValue, multiple]);

  // Sync value and only trigger onChange event when changed
  var triggerChange = useEvent(function (nextValue) {
    setMergedValue(nextValue);
    if (onChange && (nextValue === null || mergedValue.length !== nextValue.length || mergedValue.some(function (ori, index) {
      return !isSame(generateConfig, locale, ori, nextValue[index], internalPicker);
    }))) {
      onChange === null || onChange === void 0 || onChange(multiple ? nextValue : nextValue[0]);
    }
  });

  // >>> CalendarValue
  // CalendarValue is a temp value for user operation
  // which will only trigger `onCalendarChange` but not `onChange`
  var onInternalSelect = useEvent(function (newDate) {
    onSelect === null || onSelect === void 0 || onSelect(newDate);
    if (mergedMode === picker) {
      var nextValues = multiple ? toggleDates(mergedValue, newDate) : [newDate];
      triggerChange(nextValues);
    }
  });

  // >>> PickerValue
  // PickerValue is used to control the current displaying panel
  var _useMergedState5 = useMergedState(defaultPickerValue || mergedValue[0] || now, {
      value: pickerValue
    }),
    _useMergedState6 = _slicedToArray(_useMergedState5, 2),
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
        week: [].concat(_toConsumableArray(decadeYearMonthQueue), ['week']),
        date: [].concat(_toConsumableArray(decadeYearMonthQueue), ['date'])
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
      var _hoverRangeValue = _slicedToArray(hoverRangeValue, 2);
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
  var onInternalCellRender = useCellRender(cellRender, dateRender, monthCellRender);

  // ======================= Components =======================
  var PanelComponent = components[internalMode] || DefaultComponents[internalMode] || DatePanel;

  // ======================== Context =========================
  var parentHackContext = React.useContext(PickerHackContext);
  var pickerPanelContext = React.useMemo(function () {
    return _objectSpread(_objectSpread({}, parentHackContext), {}, {
      hideHeader: hideHeader
    });
  }, [parentHackContext, hideHeader]);

  // ======================== Warnings ========================
  if (process.env.NODE_ENV !== 'production') {
    warning(!mergedValue || mergedValue.every(function (val) {
      return generateConfig.isValidate(val);
    }), 'Invalidate date pass to `value` or `defaultValue`.');
  }

  // ========================= Render =========================
  var panelCls = "".concat(mergedPrefixCls, "-panel");
  var panelProps = pickProps(props, [
  // Week
  'showWeek',
  // Icons
  'prevIcon', 'nextIcon', 'superPrevIcon', 'superNextIcon',
  // Disabled
  'disabledDate', 'minDate', 'maxDate',
  // Hover
  'onHover']);
  return /*#__PURE__*/React.createElement(PickerHackContext.Provider, {
    value: pickerPanelContext
  }, /*#__PURE__*/React.createElement("div", {
    ref: rootRef,
    tabIndex: tabIndex,
    className: classNames(panelCls, _defineProperty({}, "".concat(panelCls, "-rtl"), direction === 'rtl'))
  }, /*#__PURE__*/React.createElement(PanelComponent, _extends({}, panelProps, {
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
export default RefPanelPicker;