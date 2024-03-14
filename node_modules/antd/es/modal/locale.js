import defaultLocale from '../locale/en_US';
let runtimeLocale = Object.assign({}, defaultLocale.Modal);
let localeList = [];
const generateLocale = () => localeList.reduce((merged, locale) => Object.assign(Object.assign({}, merged), locale), defaultLocale.Modal);
export function changeConfirmLocale(newLocale) {
  if (newLocale) {
    const cloneLocale = Object.assign({}, newLocale);
    localeList.push(cloneLocale);
    runtimeLocale = generateLocale();
    return () => {
      localeList = localeList.filter(locale => locale !== cloneLocale);
      runtimeLocale = generateLocale();
    };
  }
  runtimeLocale = Object.assign({}, defaultLocale.Modal);
}
export function getConfirmLocale() {
  return runtimeLocale;
}