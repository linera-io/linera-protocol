"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _hi_IN = _interopRequireDefault(require("rc-picker/lib/locale/hi_IN"));
var _hi_IN2 = _interopRequireDefault(require("../../time-picker/locale/hi_IN"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'तारीख़ चुनें',
    yearPlaceholder: 'वर्ष चुनें',
    quarterPlaceholder: 'तिमाही चुनें',
    monthPlaceholder: 'महीना चुनिए',
    weekPlaceholder: 'सप्ताह चुनें',
    rangePlaceholder: ['प्रारंभ तिथि', 'समाप्ति तिथि'],
    rangeYearPlaceholder: ['आरंभिक वर्ष', 'अंत वर्ष'],
    rangeMonthPlaceholder: ['आरंभिक महीना', 'अंत महीना'],
    rangeWeekPlaceholder: ['आरंभिक सप्ताह', 'अंत सप्ताह']
  }, _hi_IN.default),
  timePickerLocale: Object.assign({}, _hi_IN2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;