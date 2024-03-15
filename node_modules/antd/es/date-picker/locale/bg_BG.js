import CalendarLocale from "rc-picker/es/locale/bg_BG";
import TimePickerLocale from '../../time-picker/locale/bg_BG';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Избор на дата',
    rangePlaceholder: ['Начална', 'Крайна']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;