import CalendarLocale from "rc-picker/es/locale/et_EE";
import TimePickerLocale from '../../time-picker/locale/et_EE';
// 统一合并为完整的 Locale
const locale = {
  lang: Object.assign({
    placeholder: 'Vali kuupäev',
    rangePlaceholder: ['Algus kuupäev', 'Lõpu kuupäev']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;