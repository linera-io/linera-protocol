import CalendarLocale from "rc-picker/es/locale/ur_PK";
import TimePickerLocale from '../../time-picker/locale/ur_PK';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'تاریخ منتخب کریں',
    yearPlaceholder: 'سال کو منتخب کریں',
    quarterPlaceholder: 'کوارٹر منتخب کریں',
    monthPlaceholder: 'ماہ منتخب کریں',
    weekPlaceholder: 'ہفتہ منتخب کریں',
    rangePlaceholder: ['شروع کرنے کی تاریخ', 'آخری تاریخ'],
    rangeYearPlaceholder: ['آغاز سال', 'آخر سال'],
    rangeMonthPlaceholder: ['مہینہ شروع', 'اختتامی مہینہ'],
    rangeWeekPlaceholder: ['ہفتے شروع کریں', 'اختتام ہفتہ']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;