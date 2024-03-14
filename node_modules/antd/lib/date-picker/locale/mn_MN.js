"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _mn_MN = _interopRequireDefault(require("rc-picker/lib/locale/mn_MN"));
var _mn_MN2 = _interopRequireDefault(require("../../time-picker/locale/mn_MN"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Огноо сонгох',
    rangePlaceholder: ['Эхлэх огноо', 'Дуусах огноо']
  }, _mn_MN.default),
  timePickerLocale: Object.assign({}, _mn_MN2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;