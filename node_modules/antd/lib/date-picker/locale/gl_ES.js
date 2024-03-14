"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _gl_ES = _interopRequireDefault(require("rc-picker/lib/locale/gl_ES"));
var _gl_ES2 = _interopRequireDefault(require("../../time-picker/locale/gl_ES"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Escolla data',
    rangePlaceholder: ['Data inicial', 'Data final']
  }, _gl_ES.default),
  timePickerLocale: Object.assign({}, _gl_ES2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;