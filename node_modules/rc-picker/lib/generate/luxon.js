"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _luxon = require("luxon");
var weekDayFormatMap = {
  zh_CN: 'narrow',
  zh_TW: 'narrow'
};
var weekDayLengthMap = {
  en_US: 2,
  en_GB: 2
};

/**
 * Normalizes part of a moment format string that should
 * not be escaped to a luxon compatible format string.
 *
 * @param part string
 * @returns string
 */
var normalizeFormatPart = function normalizeFormatPart(part) {
  return part.replace(/Y/g, 'y').replace(/D/g, 'd').replace(/gg/g, 'kk').replace(/Q/g, 'q').replace(/([Ww])o/g, 'WW');
};

/**
 * Normalizes a moment compatible format string to a luxon compatible format string
 *
 * @param format string
 * @returns string
 */
var normalizeFormat = function normalizeFormat(format) {
  return format
  // moment escapes strings contained in brackets
  .split(/[[\]]/).map(function (part, index) {
    var shouldEscape = index % 2 > 0;
    return shouldEscape ? part : normalizeFormatPart(part);
  })
  // luxon escapes strings contained in single quotes
  .join("'");
};

/**
 * Normalizes language tags used to luxon compatible
 * language tags by replacing underscores with hyphen-minus.
 *
 * @param locale string
 * @returns string
 */
var normalizeLocale = function normalizeLocale(locale) {
  return locale.replace(/_/g, '-');
};
var generateConfig = {
  // get
  getNow: function getNow() {
    return _luxon.DateTime.local();
  },
  getFixedDate: function getFixedDate(string) {
    return _luxon.DateTime.fromFormat(string, 'yyyy-MM-dd');
  },
  getEndDate: function getEndDate(date) {
    return date.endOf('month');
  },
  getWeekDay: function getWeekDay(date) {
    return date.weekday;
  },
  getYear: function getYear(date) {
    return date.year;
  },
  getMonth: function getMonth(date) {
    return date.month - 1;
  },
  // getMonth should return 0-11, luxon month returns 1-12
  getDate: function getDate(date) {
    return date.day;
  },
  getHour: function getHour(date) {
    return date.hour;
  },
  getMinute: function getMinute(date) {
    return date.minute;
  },
  getSecond: function getSecond(date) {
    return date.second;
  },
  getMillisecond: function getMillisecond(date) {
    return date.millisecond;
  },
  // set
  addYear: function addYear(date, diff) {
    return date.plus({
      year: diff
    });
  },
  addMonth: function addMonth(date, diff) {
    return date.plus({
      month: diff
    });
  },
  addDate: function addDate(date, diff) {
    return date.plus({
      day: diff
    });
  },
  setYear: function setYear(date, year) {
    return date.set({
      year: year
    });
  },
  setMonth: function setMonth(date, month) {
    return date.set({
      month: month + 1
    });
  },
  // setMonth month argument is 0-11, luxon months are 1-12
  setDate: function setDate(date, day) {
    return date.set({
      day: day
    });
  },
  setHour: function setHour(date, hour) {
    return date.set({
      hour: hour
    });
  },
  setMinute: function setMinute(date, minute) {
    return date.set({
      minute: minute
    });
  },
  setSecond: function setSecond(date, second) {
    return date.set({
      second: second
    });
  },
  setMillisecond: function setMillisecond(date, milliseconds) {
    return date.set({
      millisecond: milliseconds
    });
  },
  // Compare
  isAfter: function isAfter(date1, date2) {
    return date1 > date2;
  },
  isValidate: function isValidate(date) {
    return date.isValid;
  },
  locale: {
    getWeekFirstDate: function getWeekFirstDate(locale, date) {
      return date.setLocale(normalizeLocale(locale)).startOf('week');
    },
    getWeekFirstDay: function getWeekFirstDay(locale) {
      return _luxon.DateTime.local().setLocale(normalizeLocale(locale)).startOf('week').weekday;
    },
    getWeek: function getWeek(locale, date) {
      return date.setLocale(normalizeLocale(locale)).weekNumber;
    },
    getShortWeekDays: function getShortWeekDays(locale) {
      var weekdays = _luxon.Info.weekdays(weekDayFormatMap[locale] || 'short', {
        locale: normalizeLocale(locale)
      });
      var shifted = weekdays.map(function (weekday) {
        return weekday.slice(0, weekDayLengthMap[locale]);
      });

      // getShortWeekDays should return weekday labels starting from Sunday.
      // luxon returns them starting from Monday, so we have to shift the results.
      shifted.unshift(shifted.pop());
      return shifted;
    },
    getShortMonths: function getShortMonths(locale) {
      return _luxon.Info.months('short', {
        locale: normalizeLocale(locale)
      });
    },
    format: function format(locale, date, _format) {
      if (!date || !date.isValid) {
        return null;
      }
      return date.setLocale(normalizeLocale(locale)).toFormat(normalizeFormat(_format));
    },
    parse: function parse(locale, text, formats) {
      for (var i = 0; i < formats.length; i += 1) {
        var normalizedFormat = normalizeFormat(formats[i]);
        var date = _luxon.DateTime.fromFormat(text, normalizedFormat, {
          locale: normalizeLocale(locale)
        });
        if (date.isValid) {
          return date;
        }
      }
      return null;
    }
  }
};
var _default = exports.default = generateConfig;