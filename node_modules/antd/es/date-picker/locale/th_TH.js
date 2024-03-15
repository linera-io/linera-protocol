import CalendarLocale from "rc-picker/es/locale/th_TH";
import TimePickerLocale from '../../time-picker/locale/th_TH';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'เลือกวันที่',
    yearPlaceholder: 'เลือกปี',
    quarterPlaceholder: 'เลือกไตรมาส',
    monthPlaceholder: 'เลือกเดือน',
    weekPlaceholder: 'เลือกสัปดาห์',
    rangePlaceholder: ['วันเริ่มต้น', 'วันสิ้นสุด'],
    rangeYearPlaceholder: ['ปีเริ่มต้น', 'ปีสิ้นสุด'],
    rangeMonthPlaceholder: ['เดือนเริ่มต้น', 'เดือนสิ้นสุด'],
    rangeWeekPlaceholder: ['สัปดาห์เริ่มต้น', 'สัปดาห์สิ้นสุด']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/blob/master/components/date-picker/locale/example.json
export default locale;