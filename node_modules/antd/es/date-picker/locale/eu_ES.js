import CalendarLocale from "rc-picker/es/locale/eu_ES";
import TimePickerLocale from '../../time-picker/locale/eu_ES';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Hautatu data',
    rangePlaceholder: ['Hasierako data', 'Amaiera data']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;