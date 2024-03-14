import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useMergedState } from 'rc-util';
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import * as React from 'react';
import { fillTime, isSame } from "../../utils/dateUtil";
export function offsetPanelDate(generateConfig, picker, date, offset) {
  switch (picker) {
    case 'date':
    case 'week':
      return generateConfig.addMonth(date, offset);
    case 'month':
    case 'quarter':
      return generateConfig.addYear(date, offset);
    case 'year':
      return generateConfig.addYear(date, offset * 10);
    case 'decade':
      return generateConfig.addYear(date, offset * 100);
    default:
      return date;
  }
}
var EMPTY_LIST = [];
export default function useRangePickerValue(generateConfig, locale, calendarValue, modes, open, activeIndex, pickerMode, multiplePanel) {
  var defaultPickerValue = arguments.length > 8 && arguments[8] !== undefined ? arguments[8] : EMPTY_LIST;
  var pickerValue = arguments.length > 9 && arguments[9] !== undefined ? arguments[9] : EMPTY_LIST;
  var timeDefaultValue = arguments.length > 10 && arguments[10] !== undefined ? arguments[10] : EMPTY_LIST;
  var onPickerValueChange = arguments.length > 11 ? arguments[11] : undefined;
  var minDate = arguments.length > 12 ? arguments[12] : undefined;
  var maxDate = arguments.length > 13 ? arguments[13] : undefined;
  var isTimePicker = pickerMode === 'time';

  // ======================== Active ========================
  // `activeIndex` must be valid to avoid getting empty `pickerValue`
  var mergedActiveIndex = activeIndex || 0;

  // ===================== Picker Value =====================
  var getDefaultPickerValue = function getDefaultPickerValue(index) {
    var now = generateConfig.getNow();
    if (isTimePicker) {
      now = fillTime(generateConfig, now);
    }
    return defaultPickerValue[index] || calendarValue[index] || now;
  };

  // Align `pickerValue` with `showTime.defaultValue`
  var _pickerValue = _slicedToArray(pickerValue, 2),
    startPickerValue = _pickerValue[0],
    endPickerValue = _pickerValue[1];

  // PickerValue state
  var _useMergedState = useMergedState(function () {
      return getDefaultPickerValue(0);
    }, {
      value: startPickerValue
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    mergedStartPickerValue = _useMergedState2[0],
    setStartPickerValue = _useMergedState2[1];
  var _useMergedState3 = useMergedState(function () {
      return getDefaultPickerValue(1);
    }, {
      value: endPickerValue
    }),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    mergedEndPickerValue = _useMergedState4[0],
    setEndPickerValue = _useMergedState4[1];

  // Current PickerValue
  var currentPickerValue = React.useMemo(function () {
    var current = [mergedStartPickerValue, mergedEndPickerValue][mergedActiveIndex];

    // Merge the `showTime.defaultValue` into `pickerValue`
    return isTimePicker ? current : fillTime(generateConfig, current, timeDefaultValue[mergedActiveIndex]);
  }, [isTimePicker, mergedStartPickerValue, mergedEndPickerValue, mergedActiveIndex, generateConfig, timeDefaultValue]);
  var setCurrentPickerValue = function setCurrentPickerValue(nextPickerValue) {
    var source = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 'panel';
    var updater = [setStartPickerValue, setEndPickerValue][mergedActiveIndex];
    updater(nextPickerValue);
    var clone = [mergedStartPickerValue, mergedEndPickerValue];
    clone[mergedActiveIndex] = nextPickerValue;
    if (onPickerValueChange && (!isSame(generateConfig, locale, mergedStartPickerValue, clone[0], pickerMode) || !isSame(generateConfig, locale, mergedEndPickerValue, clone[1], pickerMode))) {
      onPickerValueChange(clone, {
        source: source,
        range: mergedActiveIndex === 1 ? 'end' : 'start',
        mode: modes
      });
    }
  };

  // ======================== Effect ========================
  /**
   * EndDate pickerValue is little different. It should be:
   * - If date picker (without time), endDate is not same year & month as startDate
   *   - pickerValue minus one month
   * - Else pass directly
   */
  var getEndDatePickerValue = function getEndDatePickerValue(startDate, endDate) {
    if (multiplePanel) {
      // Basic offset
      var SAME_CHECKER = {
        date: 'month',
        week: 'month',
        month: 'year',
        quarter: 'year'
      };
      var mode = SAME_CHECKER[pickerMode];
      if (mode && !isSame(generateConfig, locale, startDate, endDate, mode)) {
        return offsetPanelDate(generateConfig, pickerMode, endDate, -1);
      }

      // Year offset
      if (pickerMode === 'year') {
        var srcYear = Math.floor(generateConfig.getYear(startDate) / 10);
        var tgtYear = Math.floor(generateConfig.getYear(endDate) / 10);
        if (srcYear !== tgtYear) {
          return offsetPanelDate(generateConfig, pickerMode, endDate, -1);
        }
      }
    }
    return endDate;
  };

  // >>> When switch field, reset the picker value as prev field picker value
  var prevActiveIndexRef = React.useRef(null);
  useLayoutEffect(function () {
    if (open) {
      if (!defaultPickerValue[mergedActiveIndex]) {
        var nextPickerValue = isTimePicker ? null : generateConfig.getNow();

        /**
         * 1. If has prevActiveIndex, use it to avoid panel jump
         * 2. If current field has value
         *    - If `activeIndex` is 1 and `calendarValue[0]` is not same panel as `calendarValue[1]`,
         *      offset `calendarValue[1]` and set it
         *    - Else use `calendarValue[activeIndex]`
         * 3. If current field has no value but another field has value, use another field value
         * 4. Else use now (not any `calendarValue` can ref)
         */

        if (prevActiveIndexRef.current !== null && prevActiveIndexRef.current !== mergedActiveIndex) {
          // If from another field, not jump picker value
          nextPickerValue = [mergedStartPickerValue, mergedEndPickerValue][mergedActiveIndex ^ 1];
        } else if (calendarValue[mergedActiveIndex]) {
          // Current field has value
          nextPickerValue = mergedActiveIndex === 0 ? calendarValue[0] : getEndDatePickerValue(calendarValue[0], calendarValue[1]);
        } else if (calendarValue[mergedActiveIndex ^ 1]) {
          // Current field has no value but another field has value
          nextPickerValue = calendarValue[mergedActiveIndex ^ 1];
        }

        // Only sync when has value, this will sync in the `min-max` logic
        if (nextPickerValue) {
          // nextPickerValue < minDate
          if (minDate && generateConfig.isAfter(minDate, nextPickerValue)) {
            nextPickerValue = minDate;
          }

          // maxDate < nextPickerValue
          var offsetPickerValue = multiplePanel ? offsetPanelDate(generateConfig, pickerMode, nextPickerValue, 1) : nextPickerValue;
          if (maxDate && generateConfig.isAfter(offsetPickerValue, maxDate)) {
            nextPickerValue = multiplePanel ? offsetPanelDate(generateConfig, pickerMode, maxDate, -1) : maxDate;
          }
          setCurrentPickerValue(nextPickerValue, 'reset');
        }
      }
    }
  }, [open, mergedActiveIndex, calendarValue[mergedActiveIndex]]);

  // >>> Reset prevActiveIndex when panel closed
  React.useEffect(function () {
    if (open) {
      prevActiveIndexRef.current = mergedActiveIndex;
    } else {
      prevActiveIndexRef.current = null;
    }
  }, [open, mergedActiveIndex]);

  // >>> defaultPickerValue: Resync to `defaultPickerValue` for each panel focused
  useLayoutEffect(function () {
    if (open && defaultPickerValue) {
      if (defaultPickerValue[mergedActiveIndex]) {
        setCurrentPickerValue(defaultPickerValue[mergedActiveIndex], 'reset');
      }
    }
  }, [open, mergedActiveIndex]);
  return [currentPickerValue, setCurrentPickerValue];
}