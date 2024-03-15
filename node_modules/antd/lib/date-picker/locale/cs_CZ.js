"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cs_CZ = _interopRequireDefault(require("rc-picker/lib/locale/cs_CZ"));
var _cs_CZ2 = _interopRequireDefault(require("../../time-picker/locale/cs_CZ"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Vybrat datum',
    rangePlaceholder: ['Od', 'Do']
  }, _cs_CZ.default),
  timePickerLocale: Object.assign({}, _cs_CZ2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;