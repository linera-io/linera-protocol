"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ml_IN = _interopRequireDefault(require("rc-picker/lib/locale/ml_IN"));
var _ml_IN2 = _interopRequireDefault(require("../../time-picker/locale/ml_IN"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'തിയതി തിരഞ്ഞെടുക്കുക',
    yearPlaceholder: 'വർഷം തിരഞ്ഞെടുക്കുക',
    quarterPlaceholder: 'ത്രൈമാസം തിരഞ്ഞെടുക്കുക',
    monthPlaceholder: 'മാസം തിരഞ്ഞെടുക്കുക',
    weekPlaceholder: 'വാരം തിരഞ്ഞെടുക്കുക',
    rangePlaceholder: ['ആരംഭ ദിനം', 'അവസാന ദിനം'],
    rangeYearPlaceholder: ['ആരംഭ വർഷം', 'അവസാന വർഷം'],
    rangeMonthPlaceholder: ['ആരംഭ മാസം', 'അവസാന മാസം'],
    rangeWeekPlaceholder: ['ആരംഭ വാരം', 'അവസാന വാരം']
  }, _ml_IN.default),
  timePickerLocale: Object.assign({}, _ml_IN2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;