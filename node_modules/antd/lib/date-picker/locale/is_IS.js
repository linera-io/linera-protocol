"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _is_IS = _interopRequireDefault(require("rc-picker/lib/locale/is_IS"));
var _is_IS2 = _interopRequireDefault(require("../../time-picker/locale/is_IS"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Veldu dag',
    rangePlaceholder: ['Upphafsdagur', 'Lokadagur']
  }, _is_IS.default),
  timePickerLocale: Object.assign({}, _is_IS2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;