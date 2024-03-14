import CalendarLocale from "rc-picker/es/locale/fa_IR";
import TimePickerLocale from '../../time-picker/locale/fa_IR';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'انتخاب تاریخ',
    yearPlaceholder: 'انتخاب سال',
    quarterPlaceholder: 'انتخاب فصل',
    monthPlaceholder: 'انتخاب ماه',
    weekPlaceholder: 'انتخاب هفته',
    rangePlaceholder: ['تاریخ شروع', 'تاریخ پایان'],
    rangeYearPlaceholder: ['سال شروع', 'سال پایان'],
    rangeQuarterPlaceholder: ['فصل شروع', 'فصل پایان'],
    rangeMonthPlaceholder: ['ماه شروع', 'ماه پایان'],
    rangeWeekPlaceholder: ['هفته شروع', 'هفته پایان']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;