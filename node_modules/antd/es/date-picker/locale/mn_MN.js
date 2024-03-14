import CalendarLocale from "rc-picker/es/locale/mn_MN";
import TimePickerLocale from '../../time-picker/locale/mn_MN';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Огноо сонгох',
    rangePlaceholder: ['Эхлэх огноо', 'Дуусах огноо']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;