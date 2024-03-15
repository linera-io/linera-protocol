"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ar_EG = _interopRequireDefault(require("rc-picker/lib/locale/ar_EG"));
var _ar_EG2 = _interopRequireDefault(require("../../time-picker/locale/ar_EG"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'اختيار التاريخ',
    rangePlaceholder: ['البداية', 'النهاية']
  }, _ar_EG.default),
  timePickerLocale: Object.assign({}, _ar_EG2.default),
  dateFormat: 'DD-MM-YYYY',
  monthFormat: 'MM-YYYY',
  dateTimeFormat: 'DD-MM-YYYY HH:mm:ss',
  weekFormat: 'wo-YYYY'
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;