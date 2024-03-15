"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ms_MY = _interopRequireDefault(require("rc-picker/lib/locale/ms_MY"));
var _ms_MY2 = _interopRequireDefault(require("../../time-picker/locale/ms_MY"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Pilih tarikh',
    rangePlaceholder: ['Tarikh mula', 'Tarikh akhir']
  }, _ms_MY.default),
  timePickerLocale: Object.assign({}, _ms_MY2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;