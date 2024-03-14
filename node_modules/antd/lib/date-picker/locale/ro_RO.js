"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ro_RO = _interopRequireDefault(require("rc-picker/lib/locale/ro_RO"));
var _ro_RO2 = _interopRequireDefault(require("../../time-picker/locale/ro_RO"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Selectează data',
    rangePlaceholder: ['Data start', 'Data sfârșit']
  }, _ro_RO.default),
  timePickerLocale: Object.assign({}, _ro_RO2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;