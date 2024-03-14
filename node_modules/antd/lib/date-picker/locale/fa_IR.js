"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _fa_IR = _interopRequireDefault(require("rc-picker/lib/locale/fa_IR"));
var _fa_IR2 = _interopRequireDefault(require("../../time-picker/locale/fa_IR"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'انتخاب تاریخ',
    yearPlaceholder: 'انتخاب سال',
    quarterPlaceholder: 'انتخاب فصل',
    monthPlaceholder: 'انتخاب ماه',
    weekPlaceholder: 'انتخاب هفته',
    rangePlaceholder: ['تاریخ شروع', 'تاریخ پایان'],
    rangeYearPlaceholder: ['سال شروع', 'سال پایان'],
    rangeQuarterPlaceholder: ['فصل شروع', 'فصل پایان'],
    rangeMonthPlaceholder: ['ماه شروع', 'ماه پایان'],
    rangeWeekPlaceholder: ['هفته شروع', 'هفته پایان']
  }, _fa_IR.default),
  timePickerLocale: Object.assign({}, _fa_IR2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;