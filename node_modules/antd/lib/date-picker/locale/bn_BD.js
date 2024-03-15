"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _bn_BD = _interopRequireDefault(require("rc-picker/lib/locale/bn_BD"));
var _bn_BD2 = _interopRequireDefault(require("../../time-picker/locale/bn_BD"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'তারিখ নির্বাচন',
    yearPlaceholder: 'বছর নির্বাচন',
    quarterPlaceholder: 'কোয়ার্টার নির্বাচন',
    monthPlaceholder: 'মাস নির্বাচন',
    weekPlaceholder: 'সপ্তাহ নির্বাচন',
    rangePlaceholder: ['শুরুর তারিখ', 'শেষ তারিখ'],
    rangeYearPlaceholder: ['শুরুর বছর', 'শেষ বছর'],
    rangeMonthPlaceholder: ['শুরুর মাস', 'শেষ মাস'],
    rangeWeekPlaceholder: ['শুরুর সপ্তাহ', 'শেষ সপ্তাহ']
  }, _bn_BD.default),
  timePickerLocale: Object.assign({}, _bn_BD2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;