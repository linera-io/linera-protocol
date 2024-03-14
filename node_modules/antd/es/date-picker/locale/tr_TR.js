import CalendarLocale from "rc-picker/es/locale/tr_TR";
import TimePickerLocale from '../../time-picker/locale/tr_TR';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Tarih seç',
    yearPlaceholder: 'Yıl seç',
    quarterPlaceholder: 'Çeyrek seç',
    monthPlaceholder: 'Ay seç',
    weekPlaceholder: 'Hafta seç',
    rangePlaceholder: ['Başlangıç tarihi', 'Bitiş tarihi'],
    rangeYearPlaceholder: ['Başlangıç yılı', 'Bitiş yılı'],
    rangeMonthPlaceholder: ['Başlangıç ayı', 'Bitiş ayı'],
    rangeWeekPlaceholder: ['Başlangıç haftası', 'Bitiş haftası']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;