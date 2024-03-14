import CalendarLocale from "rc-picker/es/locale/ar_EG";
import TimePickerLocale from '../../time-picker/locale/ar_EG';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'اختيار التاريخ',
    rangePlaceholder: ['البداية', 'النهاية']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale),
  dateFormat: 'DD-MM-YYYY',
  monthFormat: 'MM-YYYY',
  dateTimeFormat: 'DD-MM-YYYY HH:mm:ss',
  weekFormat: 'wo-YYYY'
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;