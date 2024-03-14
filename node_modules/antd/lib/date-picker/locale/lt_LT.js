"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _lt_LT = _interopRequireDefault(require("rc-picker/lib/locale/lt_LT"));
var _lt_LT2 = _interopRequireDefault(require("../../time-picker/locale/lt_LT"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Pasirinkite datą',
    yearPlaceholder: 'Pasirinkite metus',
    quarterPlaceholder: 'Pasirinkite ketvirtį',
    monthPlaceholder: 'Pasirinkite mėnesį',
    weekPlaceholder: 'Pasirinkite savaitę',
    rangePlaceholder: ['Pradžios data', 'Pabaigos data'],
    rangeYearPlaceholder: ['Pradžios metai', 'Pabaigos metai'],
    rangeQuarterPlaceholder: ['Pradžios ketvirtis', 'Pabaigos ketvirtis'],
    rangeMonthPlaceholder: ['Pradžios mėnesis', 'Pabaigos mėnesis'],
    rangeWeekPlaceholder: ['Pradžios savaitė', 'Pabaigos savaitė']
  }, _lt_LT.default),
  timePickerLocale: Object.assign({}, _lt_LT2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;