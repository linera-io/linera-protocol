"use strict";

var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _dateFns = require("date-fns");
var locales = _interopRequireWildcard(require("date-fns/locale"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var getLocale = function getLocale(locale) {
  return locales[locale] || locales[locale.replace(/_/g, '')] || locales[locale.replace(/_.*$/g, '')];
};
var localeParse = function localeParse(format) {
  return format.replace(/Y/g, 'y').replace(/D/g, 'd').replace(/gggg/, 'yyyy').replace(/g/g, 'G').replace(/([Ww])o/g, 'wo');
};
var generateConfig = {
  // get
  getNow: function getNow() {
    return new Date();
  },
  getFixedDate: function getFixedDate(string) {
    return new Date(string);
  },
  getEndDate: function getEndDate(date) {
    return (0, _dateFns.endOfMonth)(date);
  },
  getWeekDay: function getWeekDay(date) {
    return (0, _dateFns.getDay)(date);
  },
  getYear: function getYear(date) {
    return (0, _dateFns.getYear)(date);
  },
  getMonth: function getMonth(date) {
    return (0, _dateFns.getMonth)(date);
  },
  getDate: function getDate(date) {
    return (0, _dateFns.getDate)(date);
  },
  getHour: function getHour(date) {
    return (0, _dateFns.getHours)(date);
  },
  getMinute: function getMinute(date) {
    return (0, _dateFns.getMinutes)(date);
  },
  getSecond: function getSecond(date) {
    return (0, _dateFns.getSeconds)(date);
  },
  getMillisecond: function getMillisecond(date) {
    return date.getMilliseconds();
  },
  // set
  addYear: function addYear(date, diff) {
    return (0, _dateFns.addYears)(date, diff);
  },
  addMonth: function addMonth(date, diff) {
    return (0, _dateFns.addMonths)(date, diff);
  },
  addDate: function addDate(date, diff) {
    return (0, _dateFns.addDays)(date, diff);
  },
  setYear: function setYear(date, year) {
    return (0, _dateFns.setYear)(date, year);
  },
  setMonth: function setMonth(date, month) {
    return (0, _dateFns.setMonth)(date, month);
  },
  setDate: function setDate(date, num) {
    return (0, _dateFns.setDate)(date, num);
  },
  setHour: function setHour(date, hour) {
    return (0, _dateFns.setHours)(date, hour);
  },
  setMinute: function setMinute(date, minute) {
    return (0, _dateFns.setMinutes)(date, minute);
  },
  setSecond: function setSecond(date, second) {
    return (0, _dateFns.setSeconds)(date, second);
  },
  setMillisecond: function setMillisecond(date, millisecond) {
    return (0, _dateFns.setMilliseconds)(date, millisecond);
  },
  // Compare
  isAfter: function isAfter(date1, date2) {
    return (0, _dateFns.isAfter)(date1, date2);
  },
  isValidate: function isValidate(date) {
    return (0, _dateFns.isValid)(date);
  },
  locale: {
    getWeekFirstDay: function getWeekFirstDay(locale) {
      var clone = getLocale(locale);
      return clone.options.weekStartsOn;
    },
    getWeekFirstDate: function getWeekFirstDate(locale, date) {
      return (0, _dateFns.startOfWeek)(date, {
        locale: getLocale(locale)
      });
    },
    getWeek: function getWeek(locale, date) {
      return (0, _dateFns.getWeek)(date, {
        locale: getLocale(locale)
      });
    },
    getShortWeekDays: function getShortWeekDays(locale) {
      var clone = getLocale(locale);
      return Array.from({
        length: 7
      }).map(function (_, i) {
        return clone.localize.day(i, {
          width: 'short'
        });
      });
    },
    getShortMonths: function getShortMonths(locale) {
      var clone = getLocale(locale);
      return Array.from({
        length: 12
      }).map(function (_, i) {
        return clone.localize.month(i, {
          width: 'abbreviated'
        });
      });
    },
    format: function format(locale, date, _format) {
      if (!(0, _dateFns.isValid)(date)) {
        return null;
      }
      return (0, _dateFns.format)(date, localeParse(_format), {
        locale: getLocale(locale)
      });
    },
    parse: function parse(locale, text, formats) {
      for (var i = 0; i < formats.length; i += 1) {
        var format = localeParse(formats[i]);
        var formatText = text;
        var date = (0, _dateFns.parse)(formatText, format, new Date(), {
          locale: getLocale(locale)
        });
        if ((0, _dateFns.isValid)(date)) {
          return date;
        }
      }
      return null;
    }
  }
};
var _default = exports.default = generateConfig;