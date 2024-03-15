import CalendarLocale from "rc-picker/es/locale/pl_PL";
import TimePickerLocale from '../../time-picker/locale/pl_PL';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Wybierz datę',
    rangePlaceholder: ['Data początkowa', 'Data końcowa']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;