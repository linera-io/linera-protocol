"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _km_KH = _interopRequireDefault(require("rc-picker/lib/locale/km_KH"));
var _km_KH2 = _interopRequireDefault(require("../../time-picker/locale/km_KH"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'រើសថ្ងៃ',
    yearPlaceholder: 'រើសឆ្នាំ',
    quarterPlaceholder: 'រើសត្រីមាស',
    monthPlaceholder: 'រើសខែ',
    weekPlaceholder: 'រើសសប្តាហ៍',
    rangePlaceholder: ['ថ្ងៃចាប់ផ្ដើម', 'ថ្ងៃបញ្ចប់'],
    rangeYearPlaceholder: ['ឆ្នាំចាប់ផ្ដើម', 'ឆ្នាំបញ្ចប់'],
    rangeMonthPlaceholder: ['ខែចាប់ផ្ដើម', 'ខែបញ្ចប់'],
    rangeWeekPlaceholder: ['សប្ដាហ៍ចាប់ផ្ដើម', 'សប្ដាហ៍បញ្ចប់']
  }, _km_KH.default),
  timePickerLocale: Object.assign({}, _km_KH2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;