"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _pl_PL = _interopRequireDefault(require("rc-picker/lib/locale/pl_PL"));
var _pl_PL2 = _interopRequireDefault(require("../../time-picker/locale/pl_PL"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Wybierz datę',
    rangePlaceholder: ['Data początkowa', 'Data końcowa']
  }, _pl_PL.default),
  timePickerLocale: Object.assign({}, _pl_PL2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;