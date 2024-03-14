import CalendarLocale from "rc-picker/es/locale/is_IS";
import TimePickerLocale from '../../time-picker/locale/is_IS';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Veldu dag',
    rangePlaceholder: ['Upphafsdagur', 'Lokadagur']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;