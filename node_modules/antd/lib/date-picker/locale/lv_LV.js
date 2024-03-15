"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _lv_LV = _interopRequireDefault(require("rc-picker/lib/locale/lv_LV"));
var _lv_LV2 = _interopRequireDefault(require("../../time-picker/locale/lv_LV"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Izvēlieties datumu',
    rangePlaceholder: ['Sākuma datums', 'Beigu datums']
  }, _lv_LV.default),
  timePickerLocale: Object.assign({}, _lv_LV2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;