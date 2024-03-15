import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { warning } from 'rc-util';
import * as React from 'react';
import { findValidateTime } from "../PickerPanel/TimePanel/TimePanelBody/util";
import { leftPad } from "../utils/miscUtil";
function emptyDisabled() {
  return [];
}
function generateUnits(start, end) {
  var step = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 1;
  var hideDisabledOptions = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;
  var disabledUnits = arguments.length > 4 && arguments[4] !== undefined ? arguments[4] : [];
  var pad = arguments.length > 5 && arguments[5] !== undefined ? arguments[5] : 2;
  var units = [];
  var integerStep = step >= 1 ? step | 0 : 1;
  for (var i = start; i <= end; i += integerStep) {
    var disabled = disabledUnits.includes(i);
    if (!disabled || !hideDisabledOptions) {
      units.push({
        label: leftPad(i, pad),
        value: i,
        disabled: disabled
      });
    }
  }
  return units;
}

/**
 * Parse time props to get util info
 */
export default function useTimeInfo(generateConfig) {
  var props = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var date = arguments.length > 2 ? arguments[2] : undefined;
  var _ref = props || {},
    use12Hours = _ref.use12Hours,
    _ref$hourStep = _ref.hourStep,
    hourStep = _ref$hourStep === void 0 ? 1 : _ref$hourStep,
    _ref$minuteStep = _ref.minuteStep,
    minuteStep = _ref$minuteStep === void 0 ? 1 : _ref$minuteStep,
    _ref$secondStep = _ref.secondStep,
    secondStep = _ref$secondStep === void 0 ? 1 : _ref$secondStep,
    _ref$millisecondStep = _ref.millisecondStep,
    millisecondStep = _ref$millisecondStep === void 0 ? 100 : _ref$millisecondStep,
    hideDisabledOptions = _ref.hideDisabledOptions,
    disabledTime = _ref.disabledTime,
    disabledHours = _ref.disabledHours,
    disabledMinutes = _ref.disabledMinutes,
    disabledSeconds = _ref.disabledSeconds;
  var mergedDate = React.useMemo(function () {
    return date || generateConfig.getNow();
  }, [date, generateConfig]);

  // ======================== Warnings ========================
  if (process.env.NODE_ENV !== 'production') {
    var isHourStepValid = 24 % hourStep === 0;
    var isMinuteStepValid = 60 % minuteStep === 0;
    var isSecondStepValid = 60 % secondStep === 0;
    warning(isHourStepValid, "`hourStep` ".concat(hourStep, " is invalid. It should be a factor of 24."));
    warning(isMinuteStepValid, "`minuteStep` ".concat(minuteStep, " is invalid. It should be a factor of 60."));
    warning(isSecondStepValid, "`secondStep` ".concat(secondStep, " is invalid. It should be a factor of 60."));
  }

  // ======================== Disabled ========================
  var getDisabledTimes = React.useCallback(function (targetDate) {
    var disabledConfig = (disabledTime === null || disabledTime === void 0 ? void 0 : disabledTime(targetDate)) || {};
    return [disabledConfig.disabledHours || disabledHours || emptyDisabled, disabledConfig.disabledMinutes || disabledMinutes || emptyDisabled, disabledConfig.disabledSeconds || disabledSeconds || emptyDisabled, disabledConfig.disabledMilliseconds || emptyDisabled];
  }, [disabledTime, disabledHours, disabledMinutes, disabledSeconds]);
  var _React$useMemo = React.useMemo(function () {
      return getDisabledTimes(mergedDate);
    }, [mergedDate, getDisabledTimes]),
    _React$useMemo2 = _slicedToArray(_React$useMemo, 4),
    mergedDisabledHours = _React$useMemo2[0],
    mergedDisabledMinutes = _React$useMemo2[1],
    mergedDisabledSeconds = _React$useMemo2[2],
    mergedDisabledMilliseconds = _React$useMemo2[3];

  // ========================= Column =========================
  var getAllUnits = React.useCallback(function (getDisabledHours, getDisabledMinutes, getDisabledSeconds, getDisabledMilliseconds) {
    var hours = generateUnits(0, 23, hourStep, hideDisabledOptions, getDisabledHours());

    // Hours
    var rowHourUnits = use12Hours ? hours.map(function (unit) {
      return _objectSpread(_objectSpread({}, unit), {}, {
        label: leftPad(unit.value % 12 || 12, 2)
      });
    }) : hours;

    // Minutes
    var getMinuteUnits = function getMinuteUnits(nextHour) {
      return generateUnits(0, 59, minuteStep, hideDisabledOptions, getDisabledMinutes(nextHour));
    };

    // Seconds
    var getSecondUnits = function getSecondUnits(nextHour, nextMinute) {
      return generateUnits(0, 59, secondStep, hideDisabledOptions, getDisabledSeconds(nextHour, nextMinute));
    };

    // Milliseconds
    var getMillisecondUnits = function getMillisecondUnits(nextHour, nextMinute, nextSecond) {
      return generateUnits(0, 999, millisecondStep, hideDisabledOptions, getDisabledMilliseconds(nextHour, nextMinute, nextSecond), 3);
    };
    return [rowHourUnits, getMinuteUnits, getSecondUnits, getMillisecondUnits];
  }, [hideDisabledOptions, hourStep, use12Hours, millisecondStep, minuteStep, secondStep]);
  var _React$useMemo3 = React.useMemo(function () {
      return getAllUnits(mergedDisabledHours, mergedDisabledMinutes, mergedDisabledSeconds, mergedDisabledMilliseconds);
    }, [getAllUnits, mergedDisabledHours, mergedDisabledMinutes, mergedDisabledSeconds, mergedDisabledMilliseconds]),
    _React$useMemo4 = _slicedToArray(_React$useMemo3, 4),
    rowHourUnits = _React$useMemo4[0],
    getMinuteUnits = _React$useMemo4[1],
    getSecondUnits = _React$useMemo4[2],
    getMillisecondUnits = _React$useMemo4[3];

  // ======================== Validate ========================
  /**
   * Get validate time with `disabledTime`, `certainDate` to specific the date need to check
   */
  var getValidTime = function getValidTime(nextTime, certainDate) {
    var getCheckHourUnits = function getCheckHourUnits() {
      return rowHourUnits;
    };
    var getCheckMinuteUnits = getMinuteUnits;
    var getCheckSecondUnits = getSecondUnits;
    var getCheckMillisecondUnits = getMillisecondUnits;
    if (certainDate) {
      var _getDisabledTimes = getDisabledTimes(certainDate),
        _getDisabledTimes2 = _slicedToArray(_getDisabledTimes, 4),
        targetDisabledHours = _getDisabledTimes2[0],
        targetDisabledMinutes = _getDisabledTimes2[1],
        targetDisabledSeconds = _getDisabledTimes2[2],
        targetDisabledMilliseconds = _getDisabledTimes2[3];
      var _getAllUnits = getAllUnits(targetDisabledHours, targetDisabledMinutes, targetDisabledSeconds, targetDisabledMilliseconds),
        _getAllUnits2 = _slicedToArray(_getAllUnits, 4),
        targetRowHourUnits = _getAllUnits2[0],
        targetGetMinuteUnits = _getAllUnits2[1],
        targetGetSecondUnits = _getAllUnits2[2],
        targetGetMillisecondUnits = _getAllUnits2[3];
      getCheckHourUnits = function getCheckHourUnits() {
        return targetRowHourUnits;
      };
      getCheckMinuteUnits = targetGetMinuteUnits;
      getCheckSecondUnits = targetGetSecondUnits;
      getCheckMillisecondUnits = targetGetMillisecondUnits;
    }
    var validateDate = findValidateTime(nextTime, getCheckHourUnits, getCheckMinuteUnits, getCheckSecondUnits, getCheckMillisecondUnits, generateConfig);
    return validateDate;
  };
  return [
  // getValidTime
  getValidTime,
  // Units
  rowHourUnits, getMinuteUnits, getSecondUnits, getMillisecondUnits];
}