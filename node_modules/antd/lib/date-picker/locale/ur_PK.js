"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ur_PK = _interopRequireDefault(require("rc-picker/lib/locale/ur_PK"));
var _ur_PK2 = _interopRequireDefault(require("../../time-picker/locale/ur_PK"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'تاریخ منتخب کریں',
    yearPlaceholder: 'سال کو منتخب کریں',
    quarterPlaceholder: 'کوارٹر منتخب کریں',
    monthPlaceholder: 'ماہ منتخب کریں',
    weekPlaceholder: 'ہفتہ منتخب کریں',
    rangePlaceholder: ['شروع کرنے کی تاریخ', 'آخری تاریخ'],
    rangeYearPlaceholder: ['آغاز سال', 'آخر سال'],
    rangeMonthPlaceholder: ['مہینہ شروع', 'اختتامی مہینہ'],
    rangeWeekPlaceholder: ['ہفتے شروع کریں', 'اختتام ہفتہ']
  }, _ur_PK.default),
  timePickerLocale: Object.assign({}, _ur_PK2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;