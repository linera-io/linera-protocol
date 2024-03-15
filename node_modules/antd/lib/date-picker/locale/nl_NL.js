"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _nl_NL = _interopRequireDefault(require("rc-picker/lib/locale/nl_NL"));
var _nl_NL2 = _interopRequireDefault(require("../../time-picker/locale/nl_NL"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    monthPlaceholder: 'Selecteer maand',
    placeholder: 'Selecteer datum',
    quarterPlaceholder: 'Selecteer kwartaal',
    rangeMonthPlaceholder: ['Begin maand', 'Eind maand'],
    rangePlaceholder: ['Begin datum', 'Eind datum'],
    rangeWeekPlaceholder: ['Begin week', 'Eind week'],
    rangeYearPlaceholder: ['Begin jaar', 'Eind jaar'],
    weekPlaceholder: 'Selecteer week',
    yearPlaceholder: 'Selecteer jaar'
  }, _nl_NL.default),
  timePickerLocale: Object.assign({}, _nl_NL2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/issues/424
var _default = exports.default = locale;