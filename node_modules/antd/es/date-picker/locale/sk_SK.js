import CalendarLocale from "rc-picker/es/locale/sk_SK";
import TimePickerLocale from '../../time-picker/locale/sk_SK';
// 统一合并为完整的 Locale
const locale = {
  lang: Object.assign({
    placeholder: 'Vybrať dátum',
    rangePlaceholder: ['Od', 'Do']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;