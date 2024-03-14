import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import React from 'react';
export function fillTimeFormat(showHour, showMinute, showSecond, showMillisecond, showMeridiem) {
  var timeFormat = '';

  // Base HH:mm:ss
  var cells = [];
  if (showHour) {
    cells.push(showMeridiem ? 'hh' : 'HH');
  }
  if (showMinute) {
    cells.push('mm');
  }
  if (showSecond) {
    cells.push('ss');
  }
  timeFormat = cells.join(':');

  // Millisecond
  if (showMillisecond) {
    timeFormat += '.SSS';
  }

  // Meridiem
  if (showMeridiem) {
    timeFormat += ' A';
  }
  return timeFormat;
}

/**
 * Used for `useFilledProps` since it already in the React.useMemo
 */
function fillLocale(locale, showHour, showMinute, showSecond, showMillisecond, use12Hours) {
  // Not fill `monthFormat` since `locale.shortMonths` handle this
  // Not fill `cellMeridiemFormat` since AM & PM by default
  var fieldDateTimeFormat = locale.fieldDateTimeFormat,
    fieldDateFormat = locale.fieldDateFormat,
    fieldTimeFormat = locale.fieldTimeFormat,
    fieldMonthFormat = locale.fieldMonthFormat,
    fieldYearFormat = locale.fieldYearFormat,
    fieldWeekFormat = locale.fieldWeekFormat,
    fieldQuarterFormat = locale.fieldQuarterFormat,
    yearFormat = locale.yearFormat,
    cellYearFormat = locale.cellYearFormat,
    cellQuarterFormat = locale.cellQuarterFormat,
    dayFormat = locale.dayFormat,
    cellDateFormat = locale.cellDateFormat;
  var timeFormat = fillTimeFormat(showHour, showMinute, showSecond, showMillisecond, use12Hours);
  return _objectSpread(_objectSpread({}, locale), {}, {
    fieldDateTimeFormat: fieldDateTimeFormat || "YYYY-MM-DD ".concat(timeFormat),
    fieldDateFormat: fieldDateFormat || 'YYYY-MM-DD',
    fieldTimeFormat: fieldTimeFormat || timeFormat,
    fieldMonthFormat: fieldMonthFormat || 'YYYY-MM',
    fieldYearFormat: fieldYearFormat || 'YYYY',
    fieldWeekFormat: fieldWeekFormat || 'gggg-wo',
    fieldQuarterFormat: fieldQuarterFormat || 'YYYY-[Q]Q',
    yearFormat: yearFormat || 'YYYY',
    cellYearFormat: cellYearFormat || 'YYYY',
    cellQuarterFormat: cellQuarterFormat || '[Q]Q',
    cellDateFormat: cellDateFormat || dayFormat || 'D'
  });
}

/**
 * Fill locale format as start up
 */
export default function useLocale(locale, showProps) {
  var showHour = showProps.showHour,
    showMinute = showProps.showMinute,
    showSecond = showProps.showSecond,
    showMillisecond = showProps.showMillisecond,
    use12Hours = showProps.use12Hours;
  return React.useMemo(function () {
    return fillLocale(locale, showHour, showMinute, showSecond, showMillisecond, use12Hours);
  }, [locale, showHour, showMinute, showSecond, showMillisecond, use12Hours]);
}