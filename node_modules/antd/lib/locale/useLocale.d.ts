import type { Locale } from '.';
export type LocaleComponentName = Exclude<keyof Locale, 'locale'>;
declare const useLocale: <C extends LocaleComponentName = LocaleComponentName>(componentName: C, defaultLocale?: Locale[C] | (() => Locale[C]) | undefined) => readonly [NonNullable<Locale[C]>, string];
export default useLocale;
