import CalendarLocale from "rc-picker/es/locale/uk_UA";
import TimePickerLocale from '../../time-picker/locale/uk_UA';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Оберіть дату',
    rangePlaceholder: ['Початкова дата', 'Кінцева дата']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;