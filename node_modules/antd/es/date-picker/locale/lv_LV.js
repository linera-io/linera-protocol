import CalendarLocale from "rc-picker/es/locale/lv_LV";
import TimePickerLocale from '../../time-picker/locale/lv_LV';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Izvēlieties datumu',
    rangePlaceholder: ['Sākuma datums', 'Beigu datums']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;