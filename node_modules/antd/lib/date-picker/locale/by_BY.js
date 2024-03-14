"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _by_BY = _interopRequireDefault(require("rc-picker/lib/locale/by_BY"));
var _by_BY2 = _interopRequireDefault(require("../../time-picker/locale/by_BY"));
const locale = {
  lang: Object.assign({
    placeholder: 'Выберыце дату',
    yearPlaceholder: 'Выберыце год',
    quarterPlaceholder: 'Выберыце квартал',
    monthPlaceholder: 'Выберыце месяц',
    weekPlaceholder: 'Выберыце тыдзень',
    rangePlaceholder: ['Дата пачатку', 'Дата заканчэння'],
    rangeYearPlaceholder: ['Год пачатку', 'Год заканчэння'],
    rangeQuarterPlaceholder: ['Квартал пачатку', 'Квартал заканчэння'],
    rangeMonthPlaceholder: ['Месяц пачатку', 'Месяц заканчэння'],
    rangeWeekPlaceholder: ['Тыдзень пачаку', 'Тыдзень заканчэння']
  }, _by_BY.default),
  timePickerLocale: Object.assign({}, _by_BY2.default)
};
var _default = exports.default = locale;