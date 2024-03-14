"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _el_GR = _interopRequireDefault(require("rc-picker/lib/locale/el_GR"));
var _el_GR2 = _interopRequireDefault(require("../../time-picker/locale/el_GR"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Επιλέξτε ημερομηνία',
    rangePlaceholder: ['Αρχική ημερομηνία', 'Τελική ημερομηνία']
  }, _el_GR.default),
  timePickerLocale: Object.assign({}, _el_GR2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/issues/424
var _default = exports.default = locale;