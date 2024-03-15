"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ko_KR = _interopRequireDefault(require("rc-picker/lib/locale/ko_KR"));
var _ko_KR2 = _interopRequireDefault(require("../../time-picker/locale/ko_KR"));
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: '날짜 선택',
    rangePlaceholder: ['시작일', '종료일']
  }, _ko_KR.default),
  timePickerLocale: Object.assign({}, _ko_KR2.default)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
var _default = exports.default = locale;