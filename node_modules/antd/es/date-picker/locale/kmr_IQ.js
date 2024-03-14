import CalendarLocale from "rc-picker/es/locale/kmr_IQ";
import TimePickerLocale from '../../time-picker/locale/kmr_IQ';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Dîrok hilbijêre',
    rangePlaceholder: ['Dîroka destpêkê', 'Dîroka dawîn']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;