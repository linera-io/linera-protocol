"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _bg_BG = _interopRequireDefault(require("rc-picker/lib/locale/bg_BG"));
var _bg_BG2 = _interopRequireDefault(require("../../time-picker/locale/bg_BG"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Избор на дата',
    rangePlaceholder: ['Начална', 'Крайна']
  }, _bg_BG.default),
  timePickerLocale: Object.assign({}, _bg_BG2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;