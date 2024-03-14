"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _my_MM = _interopRequireDefault(require("rc-picker/lib/locale/my_MM"));
var _my_MM2 = _interopRequireDefault(require("../../time-picker/locale/my_MM"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'ရက်စွဲကို ရွေးပါ။',
    yearPlaceholder: 'နှစ်ကို ရွေးပါ။',
    quarterPlaceholder: 'လေးပုံတစ်ပုံကို ရွေးပါ။',
    monthPlaceholder: 'လကိုရွေးပါ။',
    weekPlaceholder: 'ရက်သတ္တပတ်ကို ရွေးပါ။',
    rangePlaceholder: ['စတင်သည့်ရက်စွဲ', 'ကုန်ဆုံးရက်'],
    rangeYearPlaceholder: ['စတင်သည့်နှစ်', 'နှစ်ကုန်'],
    rangeQuarterPlaceholder: ['လေးပုံတစ်ပုံကို စတင်ပါ။', 'အဆုံးသုံးလ'],
    rangeMonthPlaceholder: ['စတင်လ', 'လကုန်'],
    rangeWeekPlaceholder: ['ရက်သတ္တပတ်စတင်ပါ။', 'သီတင်းပတ်ကုန်']
  }, _my_MM.default),
  timePickerLocale: Object.assign({}, _my_MM2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;