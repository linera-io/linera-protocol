"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.WEEK_DAY_COUNT = void 0;
exports.fillTime = fillTime;
exports.formatValue = formatValue;
exports.getQuarter = getQuarter;
exports.getWeekStartDate = getWeekStartDate;
exports.isInRange = isInRange;
exports.isSame = isSame;
exports.isSameDate = isSameDate;
exports.isSameDecade = isSameDecade;
exports.isSameMonth = isSameMonth;
exports.isSameOrAfter = isSameOrAfter;
exports.isSameQuarter = isSameQuarter;
exports.isSameTime = isSameTime;
exports.isSameTimestamp = isSameTimestamp;
exports.isSameWeek = isSameWeek;
exports.isSameYear = isSameYear;
var WEEK_DAY_COUNT = exports.WEEK_DAY_COUNT = 7;

/**
 * Wrap the compare logic.
 * This will compare the each of value is empty first.
 * 1. All is empty, return true.
 * 2. One is empty, return false.
 * 3. return customize compare logic.
 */
function nullableCompare(value1, value2, oriCompareFn) {
  if (!value1 && !value2 || value1 === value2) {
    return true;
  }
  if (!value1 || !value2) {
    return false;
  }
  return oriCompareFn();
}
function isSameDecade(generateConfig, decade1, decade2) {
  return nullableCompare(decade1, decade2, function () {
    var num1 = Math.floor(generateConfig.getYear(decade1) / 10);
    var num2 = Math.floor(generateConfig.getYear(decade2) / 10);
    return num1 === num2;
  });
}
function isSameYear(generateConfig, year1, year2) {
  return nullableCompare(year1, year2, function () {
    return generateConfig.getYear(year1) === generateConfig.getYear(year2);
  });
}
function getQuarter(generateConfig, date) {
  var quota = Math.floor(generateConfig.getMonth(date) / 3);
  return quota + 1;
}
function isSameQuarter(generateConfig, quarter1, quarter2) {
  return nullableCompare(quarter1, quarter2, function () {
    return isSameYear(generateConfig, quarter1, quarter2) && getQuarter(generateConfig, quarter1) === getQuarter(generateConfig, quarter2);
  });
}
function isSameMonth(generateConfig, month1, month2) {
  return nullableCompare(month1, month2, function () {
    return isSameYear(generateConfig, month1, month2) && generateConfig.getMonth(month1) === generateConfig.getMonth(month2);
  });
}
function isSameDate(generateConfig, date1, date2) {
  return nullableCompare(date1, date2, function () {
    return isSameYear(generateConfig, date1, date2) && isSameMonth(generateConfig, date1, date2) && generateConfig.getDate(date1) === generateConfig.getDate(date2);
  });
}
function isSameTime(generateConfig, time1, time2) {
  return nullableCompare(time1, time2, function () {
    return generateConfig.getHour(time1) === generateConfig.getHour(time2) && generateConfig.getMinute(time1) === generateConfig.getMinute(time2) && generateConfig.getSecond(time1) === generateConfig.getSecond(time2);
  });
}

/**
 * Check if the Date is all the same of timestamp
 */
function isSameTimestamp(generateConfig, time1, time2) {
  return nullableCompare(time1, time2, function () {
    return isSameDate(generateConfig, time1, time2) && isSameTime(generateConfig, time1, time2) && generateConfig.getMillisecond(time1) === generateConfig.getMillisecond(time2);
  });
}
function isSameWeek(generateConfig, locale, date1, date2) {
  return nullableCompare(date1, date2, function () {
    var weekStartDate1 = generateConfig.locale.getWeekFirstDate(locale, date1);
    var weekStartDate2 = generateConfig.locale.getWeekFirstDate(locale, date2);
    return isSameYear(generateConfig, weekStartDate1, weekStartDate2) && generateConfig.locale.getWeek(locale, date1) === generateConfig.locale.getWeek(locale, date2);
  });
}
function isSame(generateConfig, locale, source, target, type) {
  switch (type) {
    case 'date':
      return isSameDate(generateConfig, source, target);
    case 'week':
      return isSameWeek(generateConfig, locale.locale, source, target);
    case 'month':
      return isSameMonth(generateConfig, source, target);
    case 'quarter':
      return isSameQuarter(generateConfig, source, target);
    case 'year':
      return isSameYear(generateConfig, source, target);
    case 'decade':
      return isSameDecade(generateConfig, source, target);
    case 'time':
      return isSameTime(generateConfig, source, target);
    default:
      return isSameTimestamp(generateConfig, source, target);
  }
}

/** Between in date but not equal of date */
function isInRange(generateConfig, startDate, endDate, current) {
  if (!startDate || !endDate || !current) {
    return false;
  }
  return generateConfig.isAfter(current, startDate) && generateConfig.isAfter(endDate, current);
}
function isSameOrAfter(generateConfig, locale, date1, date2, type) {
  if (isSame(generateConfig, locale, date1, date2, type)) {
    return true;
  }
  return generateConfig.isAfter(date1, date2);
}
function getWeekStartDate(locale, generateConfig, value) {
  var weekFirstDay = generateConfig.locale.getWeekFirstDay(locale);
  var monthStartDate = generateConfig.setDate(value, 1);
  var startDateWeekDay = generateConfig.getWeekDay(monthStartDate);
  var alignStartDate = generateConfig.addDate(monthStartDate, weekFirstDay - startDateWeekDay);
  if (generateConfig.getMonth(alignStartDate) === generateConfig.getMonth(value) && generateConfig.getDate(alignStartDate) > 1) {
    alignStartDate = generateConfig.addDate(alignStartDate, -7);
  }
  return alignStartDate;
}
function formatValue(value, _ref) {
  var generateConfig = _ref.generateConfig,
    locale = _ref.locale,
    format = _ref.format;
  if (!value) {
    return '';
  }
  return typeof format === 'function' ? format(value) : generateConfig.locale.format(locale.locale, value, format);
}

/**
 * Fill the time info into Date if provided.
 */
function fillTime(generateConfig, date, time) {
  var tmpDate = date;
  var getFn = ['getHour', 'getMinute', 'getSecond', 'getMillisecond'];
  var setFn = ['setHour', 'setMinute', 'setSecond', 'setMillisecond'];
  setFn.forEach(function (fn, index) {
    if (time) {
      tmpDate = generateConfig[fn](tmpDate, generateConfig[getFn[index]](time));
    } else {
      tmpDate = generateConfig[fn](tmpDate, 0);
    }
  });
  return tmpDate;
}