import CalendarLocale from "rc-picker/es/locale/he_IL";
import TimePickerLocale from '../../time-picker/locale/he_IL';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'בחר תאריך',
    rangePlaceholder: ['תאריך התחלה', 'תאריך סיום']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;