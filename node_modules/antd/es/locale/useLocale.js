import * as React from 'react';
import LocaleContext from './context';
import defaultLocaleData from './en_US';
const useLocale = (componentName, defaultLocale) => {
  const fullLocale = React.useContext(LocaleContext);
  const getLocale = React.useMemo(() => {
    var _a;
    const locale = defaultLocale || defaultLocaleData[componentName];
    const localeFromContext = (_a = fullLocale === null || fullLocale === void 0 ? void 0 : fullLocale[componentName]) !== null && _a !== void 0 ? _a : {};
    return Object.assign(Object.assign({}, typeof locale === 'function' ? locale() : locale), localeFromContext || {});
  }, [componentName, defaultLocale, fullLocale]);
  const getLocaleCode = React.useMemo(() => {
    const localeCode = fullLocale === null || fullLocale === void 0 ? void 0 : fullLocale.locale;
    // Had use LocaleProvide but didn't set locale
    if ((fullLocale === null || fullLocale === void 0 ? void 0 : fullLocale.exist) && !localeCode) {
      return defaultLocaleData.locale;
    }
    return localeCode;
  }, [fullLocale]);
  return [getLocale, getLocaleCode];
};
export default useLocale;