"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ka_GE = _interopRequireDefault(require("rc-picker/lib/locale/ka_GE"));
var _ka_GE2 = _interopRequireDefault(require("../../time-picker/locale/ka_GE"));
const locale = {
  lang: Object.assign({
    placeholder: 'აირჩიეთ თარიღი',
    yearPlaceholder: 'აირჩიეთ წელი',
    quarterPlaceholder: 'აირჩიეთ მეოთხედი',
    monthPlaceholder: 'აირჩიეთ თვე',
    weekPlaceholder: 'აირჩიეთ კვირა',
    rangePlaceholder: ['საწყისი თარიღი', 'საბოლოო თარიღი'],
    rangeYearPlaceholder: ['საწყისი წელი', 'საბოლოო წელი'],
    rangeMonthPlaceholder: ['საწყისი თვე', 'საბოლოო თვე'],
    rangeWeekPlaceholder: ['საწყისი კვირა', 'საბოლოო კვირა']
  }, _ka_GE.default),
  timePickerLocale: Object.assign({}, _ka_GE2.default)
};
var _default = exports.default = locale;