import CalendarLocale from "rc-picker/es/locale/hu_HU";
import TimePickerLocale from '../../time-picker/locale/hu_HU';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Válasszon dátumot',
    rangePlaceholder: ['Kezdő dátum', 'Befejezés dátuma']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;