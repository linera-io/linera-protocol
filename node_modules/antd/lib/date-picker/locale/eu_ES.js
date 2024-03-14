"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _eu_ES = _interopRequireDefault(require("rc-picker/lib/locale/eu_ES"));
var _eu_ES2 = _interopRequireDefault(require("../../time-picker/locale/eu_ES"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Hautatu data',
    rangePlaceholder: ['Hasierako data', 'Amaiera data']
  }, _eu_ES.default),
  timePickerLocale: Object.assign({}, _eu_ES2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;