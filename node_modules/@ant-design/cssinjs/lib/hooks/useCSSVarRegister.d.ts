import type { TokenWithCSSVar } from '../util/css-variables';
import type { ExtractStyle } from './useGlobalCache';
export declare const CSS_VAR_PREFIX = "cssVar";
type CSSVarCacheValue<V, T extends Record<string, V> = Record<string, V>> = [
    cssVarToken: TokenWithCSSVar<V, T>,
    cssVarStr: string,
    styleId: string,
    cssVarKey: string
];
declare const useCSSVarRegister: <V, T extends Record<string, V>>(config: {
    path: string[];
    key: string;
    prefix?: string;
    unitless?: Record<string, boolean>;
    ignore?: Record<string, boolean>;
    scope?: string;
    token: any;
}, fn: () => T) => CSSVarCacheValue<V, T>;
export declare const extract: ExtractStyle<CSSVarCacheValue<any>>;
export default useCSSVarRegister;
