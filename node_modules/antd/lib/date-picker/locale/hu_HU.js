"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _hu_HU = _interopRequireDefault(require("rc-picker/lib/locale/hu_HU"));
var _hu_HU2 = _interopRequireDefault(require("../../time-picker/locale/hu_HU"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Válasszon dátumot',
    rangePlaceholder: ['Kezdő dátum', 'Befejezés dátuma']
  }, _hu_HU.default),
  timePickerLocale: Object.assign({}, _hu_HU2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;