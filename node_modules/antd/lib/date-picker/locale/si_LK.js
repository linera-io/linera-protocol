"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _si_LK = _interopRequireDefault(require("rc-picker/lib/locale/si_LK"));
var _si_LK2 = _interopRequireDefault(require("../../time-picker/locale/si_LK"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'දිනය තෝරන්න',
    yearPlaceholder: 'අවුරුද්ද තෝරන්න',
    quarterPlaceholder: 'කාර්තුව තෝරන්න',
    monthPlaceholder: 'මාසය තෝරන්න',
    weekPlaceholder: 'සතිය තෝරන්න',
    rangePlaceholder: ['ආරම්භක දිනය', 'නිමවන දිනය'],
    rangeYearPlaceholder: ['ආර්ම්භක අවුරුද්ද', 'නිමවන අවුරුද්ද'],
    rangeQuarterPlaceholder: ['ආරම්භක කාර්තුව', 'නිමවන කාර්තුව'],
    rangeMonthPlaceholder: ['ආරම්භක මාසය', 'නිමවන මාසය'],
    rangeWeekPlaceholder: ['ආරම්භක සතිය', 'නිමවන සතිය']
  }, _si_LK.default),
  timePickerLocale: Object.assign({}, _si_LK2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;