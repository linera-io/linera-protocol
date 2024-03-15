"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _dayjs = _interopRequireDefault(require("dayjs"));
var _warning = require("rc-util/lib/warning");
var _weekday = _interopRequireDefault(require("dayjs/plugin/weekday"));
var _localeData = _interopRequireDefault(require("dayjs/plugin/localeData"));
var _weekOfYear = _interopRequireDefault(require("dayjs/plugin/weekOfYear"));
var _weekYear = _interopRequireDefault(require("dayjs/plugin/weekYear"));
var _advancedFormat = _interopRequireDefault(require("dayjs/plugin/advancedFormat"));
var _customParseFormat = _interopRequireDefault(require("dayjs/plugin/customParseFormat"));
_dayjs.default.extend(_customParseFormat.default);
_dayjs.default.extend(_advancedFormat.default);
_dayjs.default.extend(_weekday.default);
_dayjs.default.extend(_localeData.default);
_dayjs.default.extend(_weekOfYear.default);
_dayjs.default.extend(_weekYear.default);
_dayjs.default.extend(function (o, c) {
  // todo support Wo (ISO week)
  var proto = c.prototype;
  var oldFormat = proto.format;
  proto.format = function f(formatStr) {
    var str = (formatStr || '').replace('Wo', 'wo');
    return oldFormat.bind(this)(str);
  };
});
var localeMap = {
  // ar_EG:
  // az_AZ:
  // bg_BG:
  bn_BD: 'bn-bd',
  by_BY: 'be',
  // ca_ES:
  // cs_CZ:
  // da_DK:
  // de_DE:
  // el_GR:
  en_GB: 'en-gb',
  en_US: 'en',
  // es_ES:
  // et_EE:
  // fa_IR:
  // fi_FI:
  fr_BE: 'fr',
  // todo: dayjs has no fr_BE locale, use fr at present
  fr_CA: 'fr-ca',
  // fr_FR:
  // ga_IE:
  // gl_ES:
  // he_IL:
  // hi_IN:
  // hr_HR:
  // hu_HU:
  hy_AM: 'hy-am',
  // id_ID:
  // is_IS:
  // it_IT:
  // ja_JP:
  // ka_GE:
  // kk_KZ:
  // km_KH:
  kmr_IQ: 'ku',
  // kn_IN:
  // ko_KR:
  // ku_IQ: // previous ku in antd
  // lt_LT:
  // lv_LV:
  // mk_MK:
  // ml_IN:
  // mn_MN:
  // ms_MY:
  // nb_NO:
  // ne_NP:
  nl_BE: 'nl-be',
  // nl_NL:
  // pl_PL:
  pt_BR: 'pt-br',
  // pt_PT:
  // ro_RO:
  // ru_RU:
  // sk_SK:
  // sl_SI:
  // sr_RS:
  // sv_SE:
  // ta_IN:
  // th_TH:
  // tr_TR:
  // uk_UA:
  // ur_PK:
  // vi_VN:
  zh_CN: 'zh-cn',
  zh_HK: 'zh-hk',
  zh_TW: 'zh-tw'
};
var parseLocale = function parseLocale(locale) {
  var mapLocale = localeMap[locale];
  return mapLocale || locale.split('_')[0];
};
var parseNoMatchNotice = function parseNoMatchNotice() {
  /* istanbul ignore next */
  (0, _warning.noteOnce)(false, 'Not match any format. Please help to fire a issue about this.');
};
var generateConfig = {
  // get
  getNow: function getNow() {
    return (0, _dayjs.default)();
  },
  getFixedDate: function getFixedDate(string) {
    return (0, _dayjs.default)(string, ['YYYY-M-DD', 'YYYY-MM-DD']);
  },
  getEndDate: function getEndDate(date) {
    return date.endOf('month');
  },
  getWeekDay: function getWeekDay(date) {
    var clone = date.locale('en');
    return clone.weekday() + clone.localeData().firstDayOfWeek();
  },
  getYear: function getYear(date) {
    return date.year();
  },
  getMonth: function getMonth(date) {
    return date.month();
  },
  getDate: function getDate(date) {
    return date.date();
  },
  getHour: function getHour(date) {
    return date.hour();
  },
  getMinute: function getMinute(date) {
    return date.minute();
  },
  getSecond: function getSecond(date) {
    return date.second();
  },
  getMillisecond: function getMillisecond(date) {
    return date.millisecond();
  },
  // set
  addYear: function addYear(date, diff) {
    return date.add(diff, 'year');
  },
  addMonth: function addMonth(date, diff) {
    return date.add(diff, 'month');
  },
  addDate: function addDate(date, diff) {
    return date.add(diff, 'day');
  },
  setYear: function setYear(date, year) {
    return date.year(year);
  },
  setMonth: function setMonth(date, month) {
    return date.month(month);
  },
  setDate: function setDate(date, num) {
    return date.date(num);
  },
  setHour: function setHour(date, hour) {
    return date.hour(hour);
  },
  setMinute: function setMinute(date, minute) {
    return date.minute(minute);
  },
  setSecond: function setSecond(date, second) {
    return date.second(second);
  },
  setMillisecond: function setMillisecond(date, milliseconds) {
    return date.millisecond(milliseconds);
  },
  // Compare
  isAfter: function isAfter(date1, date2) {
    return date1.isAfter(date2);
  },
  isValidate: function isValidate(date) {
    return date.isValid();
  },
  locale: {
    getWeekFirstDay: function getWeekFirstDay(locale) {
      return (0, _dayjs.default)().locale(parseLocale(locale)).localeData().firstDayOfWeek();
    },
    getWeekFirstDate: function getWeekFirstDate(locale, date) {
      return date.locale(parseLocale(locale)).weekday(0);
    },
    getWeek: function getWeek(locale, date) {
      return date.locale(parseLocale(locale)).week();
    },
    getShortWeekDays: function getShortWeekDays(locale) {
      return (0, _dayjs.default)().locale(parseLocale(locale)).localeData().weekdaysMin();
    },
    getShortMonths: function getShortMonths(locale) {
      return (0, _dayjs.default)().locale(parseLocale(locale)).localeData().monthsShort();
    },
    format: function format(locale, date, _format) {
      return date.locale(parseLocale(locale)).format(_format);
    },
    parse: function parse(locale, text, formats) {
      var localeStr = parseLocale(locale);
      for (var i = 0; i < formats.length; i += 1) {
        var format = formats[i];
        var formatText = text;
        if (format.includes('wo') || format.includes('Wo')) {
          // parse Wo
          var year = formatText.split('-')[0];
          var weekStr = formatText.split('-')[1];
          var firstWeek = (0, _dayjs.default)(year, 'YYYY').startOf('year').locale(localeStr);
          for (var j = 0; j <= 52; j += 1) {
            var nextWeek = firstWeek.add(j, 'week');
            if (nextWeek.format('Wo') === weekStr) {
              return nextWeek;
            }
          }
          parseNoMatchNotice();
          return null;
        }
        var date = (0, _dayjs.default)(formatText, format, true).locale(localeStr);
        if (date.isValid()) {
          return date;
        }
      }
      if (text) {
        parseNoMatchNotice();
      }
      return null;
    }
  }
};
var _default = exports.default = generateConfig;