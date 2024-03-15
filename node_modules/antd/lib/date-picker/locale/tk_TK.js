"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _tk_TK = _interopRequireDefault(require("rc-picker/lib/locale/tk_TK"));
var _tk_TK2 = _interopRequireDefault(require("../../time-picker/locale/tk_TK"));
const locale = {
  lang: Object.assign({
    placeholder: 'Wagt saýlaň',
    rangePlaceholder: ['Başlanýan wagty', 'Gutarýan wagty'],
    yearPlaceholder: 'Ýyl saýlaň',
    quarterPlaceholder: 'Çärýek saýlaň',
    monthPlaceholder: 'Aý saýlaň',
    weekPlaceholder: 'Hepde saýlaň',
    rangeYearPlaceholder: ['Başlanýan ýyly', 'Gutarýan ýyly'],
    rangeQuarterPlaceholder: ['Başlanýan çärýegi', 'Gutarýan çärýegi'],
    rangeMonthPlaceholder: ['Başlanýan aýy', 'Gutarýan aýy'],
    rangeWeekPlaceholder: ['Başlanýan hepdesi', 'Gutarýan hepdesi']
  }, _tk_TK.default),
  timePickerLocale: Object.assign({}, _tk_TK2.default)
};
var _default = exports.default = locale;