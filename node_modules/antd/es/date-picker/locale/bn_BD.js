import CalendarLocale from "rc-picker/es/locale/bn_BD";
import TimePickerLocale from '../../time-picker/locale/bn_BD';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'তারিখ নির্বাচন',
    yearPlaceholder: 'বছর নির্বাচন',
    quarterPlaceholder: 'কোয়ার্টার নির্বাচন',
    monthPlaceholder: 'মাস নির্বাচন',
    weekPlaceholder: 'সপ্তাহ নির্বাচন',
    rangePlaceholder: ['শুরুর তারিখ', 'শেষ তারিখ'],
    rangeYearPlaceholder: ['শুরুর বছর', 'শেষ বছর'],
    rangeMonthPlaceholder: ['শুরুর মাস', 'শেষ মাস'],
    rangeWeekPlaceholder: ['শুরুর সপ্তাহ', 'শেষ সপ্তাহ']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;