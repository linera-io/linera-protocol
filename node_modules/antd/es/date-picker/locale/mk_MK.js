import CalendarLocale from "rc-picker/es/locale/mk_MK";
import TimePickerLocale from '../../time-picker/locale/mk_MK';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Избери датум',
    rangePlaceholder: ['Од датум', 'До датум']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;