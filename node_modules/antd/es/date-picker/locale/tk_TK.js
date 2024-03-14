import CalendarLocale from "rc-picker/es/locale/tk_TK";
import TimePickerLocale from '../../time-picker/locale/tk_TK';
const locale = {
  lang: Object.assign({
    placeholder: 'Wagt saýlaň',
    rangePlaceholder: ['Başlanýan wagty', 'Gutarýan wagty'],
    yearPlaceholder: 'Ýyl saýlaň',
    quarterPlaceholder: 'Çärýek saýlaň',
    monthPlaceholder: 'Aý saýlaň',
    weekPlaceholder: 'Hepde saýlaň',
    rangeYearPlaceholder: ['Başlanýan ýyly', 'Gutarýan ýyly'],
    rangeQuarterPlaceholder: ['Başlanýan çärýegi', 'Gutarýan çärýegi'],
    rangeMonthPlaceholder: ['Başlanýan aýy', 'Gutarýan aýy'],
    rangeWeekPlaceholder: ['Başlanýan hepdesi', 'Gutarýan hepdesi']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
export default locale;