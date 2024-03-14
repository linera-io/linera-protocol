import CalendarLocale from "rc-picker/es/locale/fr_BE";
import TimePickerLocale from '../../time-picker/locale/fr_BE';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Sélectionner une date',
    yearPlaceholder: 'Sélectionner une année',
    quarterPlaceholder: 'Sélectionner un trimestre',
    monthPlaceholder: 'Sélectionner un mois',
    weekPlaceholder: 'Sélectionner une semaine',
    rangePlaceholder: ['Date de début', 'Date de fin'],
    rangeYearPlaceholder: ['Année de début', 'Année de fin'],
    rangeMonthPlaceholder: ['Mois de début', 'Mois de fin'],
    rangeWeekPlaceholder: ['Semaine de début', 'Semaine de fin']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/issues/424
export default locale;