import CalendarLocale from "rc-picker/es/locale/az_AZ";
import TimePickerLocale from '../../time-picker/locale/az_AZ';
const locale = {
  lang: Object.assign({
    placeholder: 'Tarix seçin',
    rangePlaceholder: ['Başlama tarixi', 'Bitmə tarixi']
  }, CalendarLocale),
  timePickerLocale: Object.assign({}, TimePickerLocale)
};
export default locale;