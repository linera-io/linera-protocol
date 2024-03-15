import CalendarLocale from "rc-picker/es/locale/el_GR";
import TimePickerLocale from '../../time-picker/locale/el_GR';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Επιλέξτε ημερομηνία',
    rangePlaceholder: ['Αρχική ημερομηνία', 'Τελική ημερομηνία']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/issues/424
export default locale;