"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useRangeValue;
exports.useCalendarValue = useCalendarValue;
exports.useInnerValue = useInnerValue;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _rcUtil = require("rc-util");
var React = _interopRequireWildcard(require("react"));
var _useSyncState5 = _interopRequireDefault(require("../../hooks/useSyncState"));
var _dateUtil = require("../../utils/dateUtil");
var _miscUtil = require("../../utils/miscUtil");
var _useLockEffect = _interopRequireDefault(require("./useLockEffect"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var EMPTY_VALUE = [];

// Submit Logic:
// * ✅ Value:
//    * merged value using controlled value, if not, use stateValue
//    * When merged value change, [1] resync calendar value and submit value
// * ✅ Calender Value:
//    * 💻 When user typing is validate, change the calendar value
//    * 🌅 When user click on the panel, change the calendar value
// * Submit Value:
//    * 💻 When user blur the input, flush calendar value to submit value
//    * 🌅 When user click on the panel is no needConfirm, flush calendar value to submit value
//    * 🌅 When user click on the panel is needConfirm and click OK, flush calendar value to submit value
// * Blur logic & close logic:
//    * ✅ For value, always try flush submit
//    * ✅ If `needConfirm`, reset as [1]
//    * Else (`!needConfirm`)
//      * If has another index field, active another index
// * ✅ Flush submit:
//    * If all the start & end field is confirmed or all blur or panel closed
//    * Update `needSubmit` mark to true
//    * trigger onChange by `needSubmit` and update stateValue

function useUtil(generateConfig, locale, formatList) {
  var getDateTexts = function getDateTexts(dates) {
    return dates.map(function (date) {
      return (0, _dateUtil.formatValue)(date, {
        generateConfig: generateConfig,
        locale: locale,
        format: formatList[0]
      });
    });
  };
  var isSameDates = function isSameDates(source, target) {
    var maxLen = Math.max(source.length, target.length);
    var diffIndex = -1;
    for (var i = 0; i < maxLen; i += 1) {
      var prev = source[i] || null;
      var next = target[i] || null;
      if (prev !== next && !(0, _dateUtil.isSameTimestamp)(generateConfig, prev, next)) {
        diffIndex = i;
        break;
      }
    }
    return [diffIndex < 0, diffIndex !== 0];
  };
  return [getDateTexts, isSameDates];
}
function orderDates(dates, generateConfig) {
  return (0, _toConsumableArray2.default)(dates).sort(function (a, b) {
    return generateConfig.isAfter(a, b) ? 1 : -1;
  });
}

/**
 * Used for internal value management.
 * It should always use `mergedValue` in render logic
 */
function useCalendarValue(mergedValue) {
  var _useSyncState = (0, _useSyncState5.default)(mergedValue),
    _useSyncState2 = (0, _slicedToArray2.default)(_useSyncState, 2),
    calendarValue = _useSyncState2[0],
    setCalendarValue = _useSyncState2[1];

  /** Sync calendarValue & submitValue back with value */
  var syncWithValue = (0, _rcUtil.useEvent)(function () {
    setCalendarValue(mergedValue);
  });
  React.useEffect(function () {
    syncWithValue();
  }, [mergedValue]);
  return [calendarValue, setCalendarValue];
}

/**
 * Control the internal `value` align with prop `value` and provide a temp `calendarValue` for ui.
 * `calendarValue` will be reset when blur & focus & open.
 */
function useInnerValue(generateConfig, locale, formatList, /** Used for RangePicker. `true` means [DateType, DateType] or will be DateType[] */
rangeValue,
/**
 * Trigger order when trigger calendar value change.
 * This should only used in SinglePicker with `multiple` mode.
 * So when `rangeValue` is `true`, order will be ignored.
 */
order, defaultValue, value, onCalendarChange, onOk) {
  // This is the root value which will sync with controlled or uncontrolled value
  var _useMergedState = (0, _rcUtil.useMergedState)(defaultValue, {
      value: value
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    innerValue = _useMergedState2[0],
    setInnerValue = _useMergedState2[1];
  var mergedValue = innerValue || EMPTY_VALUE;

  // ========================= Inner Values =========================
  var _useCalendarValue = useCalendarValue(mergedValue),
    _useCalendarValue2 = (0, _slicedToArray2.default)(_useCalendarValue, 2),
    calendarValue = _useCalendarValue2[0],
    setCalendarValue = _useCalendarValue2[1];

  // ============================ Change ============================
  var _useUtil = useUtil(generateConfig, locale, formatList),
    _useUtil2 = (0, _slicedToArray2.default)(_useUtil, 2),
    getDateTexts = _useUtil2[0],
    isSameDates = _useUtil2[1];
  var triggerCalendarChange = (0, _rcUtil.useEvent)(function (nextCalendarValues) {
    var clone = (0, _toConsumableArray2.default)(nextCalendarValues);
    if (rangeValue) {
      for (var i = 0; i < 2; i += 1) {
        clone[i] = clone[i] || null;
      }
    } else if (order) {
      clone = orderDates(clone.filter(function (date) {
        return date;
      }), generateConfig);
    }

    // Update merged value
    var _isSameDates = isSameDates(calendarValue(), clone),
      _isSameDates2 = (0, _slicedToArray2.default)(_isSameDates, 2),
      isSameMergedDates = _isSameDates2[0],
      isSameStart = _isSameDates2[1];
    if (!isSameMergedDates) {
      setCalendarValue(clone);

      // Trigger calendar change event
      if (onCalendarChange) {
        var cellTexts = getDateTexts(clone);
        onCalendarChange(clone, cellTexts, {
          range: isSameStart ? 'end' : 'start'
        });
      }
    }
  });
  var triggerOk = function triggerOk() {
    if (onOk) {
      onOk(calendarValue());
    }
  };
  return [mergedValue, setInnerValue, calendarValue, triggerCalendarChange, triggerOk];
}
function useRangeValue(info, mergedValue, setInnerValue, getCalendarValue, triggerCalendarChange, disabled, formatList, focused, open, isInvalidateDate) {
  var generateConfig = info.generateConfig,
    locale = info.locale,
    picker = info.picker,
    onChange = info.onChange,
    allowEmpty = info.allowEmpty,
    order = info.order;
  var orderOnChange = disabled.some(function (d) {
    return d;
  }) ? false : order;

  // ============================= Util =============================
  var _useUtil3 = useUtil(generateConfig, locale, formatList),
    _useUtil4 = (0, _slicedToArray2.default)(_useUtil3, 2),
    getDateTexts = _useUtil4[0],
    isSameDates = _useUtil4[1];

  // ============================ Values ============================
  // Used for trigger `onChange` event.
  // Record current value which is wait for submit.
  var _useSyncState3 = (0, _useSyncState5.default)(mergedValue),
    _useSyncState4 = (0, _slicedToArray2.default)(_useSyncState3, 2),
    submitValue = _useSyncState4[0],
    setSubmitValue = _useSyncState4[1];

  /** Sync calendarValue & submitValue back with value */
  var syncWithValue = (0, _rcUtil.useEvent)(function () {
    setSubmitValue(mergedValue);
  });
  React.useEffect(function () {
    syncWithValue();
  }, [mergedValue]);

  // ============================ Submit ============================
  var triggerSubmit = (0, _rcUtil.useEvent)(function (nextValue) {
    var isNullValue = nextValue === null;
    var clone = (0, _toConsumableArray2.default)(nextValue || submitValue());

    // Fill null value
    if (isNullValue) {
      var maxLen = Math.max(disabled.length, clone.length);
      for (var i = 0; i < maxLen; i += 1) {
        if (!disabled[i]) {
          clone[i] = null;
        }
      }
    }

    // Only when exist value to sort
    if (orderOnChange && clone[0] && clone[1]) {
      clone = orderDates(clone, generateConfig);
    }

    // Sync `calendarValue`
    triggerCalendarChange(clone);

    // ========= Validate check =========
    var _clone = clone,
      _clone2 = (0, _slicedToArray2.default)(_clone, 2),
      start = _clone2[0],
      end = _clone2[1];

    // >>> Empty
    var startEmpty = !start;
    var endEmpty = !end;
    var validateEmptyDateRange = allowEmpty ?
    // Validate empty start
    (!startEmpty || allowEmpty[0]) && (
    // Validate empty end
    !endEmpty || allowEmpty[1]) : true;

    // >>> Order
    var validateOrder = !order || startEmpty || endEmpty || (0, _dateUtil.isSame)(generateConfig, locale, start, end, picker) || generateConfig.isAfter(end, start);

    // >>> Invalid
    var validateDates =
    // Validate start
    (!start || !isInvalidateDate(start, {
      activeIndex: 0
    })) && (
    // Validate end
    !end || !isInvalidateDate(end, {
      from: start,
      activeIndex: 1
    }));

    // >>> Result
    var allPassed =
    // Null value is from clear button
    isNullValue ||
    // Normal check
    validateEmptyDateRange && validateOrder && validateDates;
    if (allPassed) {
      // Sync value with submit value
      setInnerValue(clone);
      var _isSameDates3 = isSameDates(clone, mergedValue),
        _isSameDates4 = (0, _slicedToArray2.default)(_isSameDates3, 1),
        isSameMergedDates = _isSameDates4[0];

      // Trigger `onChange` if needed
      if (onChange && !isSameMergedDates) {
        onChange(
        // Return null directly if all date are empty
        isNullValue && clone.every(function (val) {
          return !val;
        }) ? null : clone, getDateTexts(clone));
      }
    }
    return allPassed;
  });

  // ========================= Flush Submit =========================
  var flushSubmit = (0, _rcUtil.useEvent)(function (index, needTriggerChange) {
    var nextSubmitValue = (0, _miscUtil.fillIndex)(submitValue(), index, getCalendarValue()[index]);
    setSubmitValue(nextSubmitValue);
    if (needTriggerChange) {
      triggerSubmit();
    }
  });

  // ============================ Effect ============================
  // All finished action trigger after 2 frames
  var interactiveFinished = !focused && !open;
  (0, _useLockEffect.default)(!interactiveFinished, function () {
    if (interactiveFinished) {
      // Always try to trigger submit first
      triggerSubmit();

      // Trigger calendar change since this is a effect reset
      // https://github.com/ant-design/ant-design/issues/22351
      triggerCalendarChange(mergedValue);

      // Sync with value anyway
      syncWithValue();
    }
  }, 2);

  // ============================ Return ============================
  return [flushSubmit, triggerSubmit];
}