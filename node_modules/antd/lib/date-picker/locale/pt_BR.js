"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _pt_BR = _interopRequireDefault(require("rc-picker/lib/locale/pt_BR"));
var _pt_BR2 = _interopRequireDefault(require("../../time-picker/locale/pt_BR"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Selecionar data',
    rangePlaceholder: ['Data inicial', 'Data final']
  }, _pt_BR.default),
  timePickerLocale: Object.assign({}, _pt_BR2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;