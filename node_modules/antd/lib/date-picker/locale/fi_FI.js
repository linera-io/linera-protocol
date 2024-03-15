"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _fi_FI = _interopRequireDefault(require("rc-picker/lib/locale/fi_FI"));
var _fi_FI2 = _interopRequireDefault(require("../../time-picker/locale/fi_FI"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Valitse päivä',
    rangePlaceholder: ['Alkamispäivä', 'Päättymispäivä']
  }, _fi_FI.default),
  timePickerLocale: Object.assign({}, _fi_FI2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;