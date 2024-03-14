/// <reference types="react" />
import type { Locale } from '.';
export type LocaleContextProps = Locale & {
    exist?: boolean;
};
declare const LocaleContext: import("react").Context<LocaleContextProps | undefined>;
export default LocaleContext;
