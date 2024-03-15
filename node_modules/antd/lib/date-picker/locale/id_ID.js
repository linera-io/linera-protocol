"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _id_ID = _interopRequireDefault(require("rc-picker/lib/locale/id_ID"));
var _id_ID2 = _interopRequireDefault(require("../../time-picker/locale/id_ID"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Pilih tanggal',
    rangePlaceholder: ['Mulai tanggal', 'Tanggal akhir']
  }, _id_ID.default),
  timePickerLocale: Object.assign({}, _id_ID2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;