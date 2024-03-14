"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _sk_SK = _interopRequireDefault(require("rc-picker/lib/locale/sk_SK"));
var _sk_SK2 = _interopRequireDefault(require("../../time-picker/locale/sk_SK"));
// 统一合并为完整的 Locale
const locale = {
  lang: Object.assign({
    placeholder: 'Vybrať dátum',
    rangePlaceholder: ['Od', 'Do']
  }, _sk_SK.default),
  timePickerLocale: Object.assign({}, _sk_SK2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;