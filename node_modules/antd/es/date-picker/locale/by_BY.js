import CalendarLocale from "rc-picker/es/locale/by_BY";
import TimePickerLocale from '../../time-picker/locale/by_BY';
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
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
export default locale;