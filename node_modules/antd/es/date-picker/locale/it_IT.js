import CalendarLocale from "rc-picker/es/locale/it_IT";
import TimePickerLocale from '../../time-picker/locale/it_IT';
// Merge into a locale object
const locale = {
  lang: Object.assign({
    placeholder: 'Selezionare la data',
    rangePlaceholder: ["Data d'inizio", 'Data di fine']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
// All settings at:
// https://github.com/ant-design/ant-design/issues/424
export default locale;