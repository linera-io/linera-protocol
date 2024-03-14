import CalendarLocale from "rc-picker/es/locale/nl_NL";
import TimePickerLocale from '../../time-picker/locale/nl_NL';
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
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/issues/424
export default locale;