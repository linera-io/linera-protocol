"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _es_ES = _interopRequireDefault(require("rc-picker/lib/locale/es_ES"));
var _es_ES2 = _interopRequireDefault(require("../../time-picker/locale/es_ES"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Seleccionar fecha',
    rangePlaceholder: ['Fecha inicial', 'Fecha final']
  }, _es_ES.default),
  timePickerLocale: Object.assign({}, _es_ES2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;