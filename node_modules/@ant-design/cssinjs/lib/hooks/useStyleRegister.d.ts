import type * as CSS from 'csstype';
import * as React from 'react';
import type { Theme, Transformer } from '..';
import type Keyframes from '../Keyframes';
import type { Linter } from '../linters';
import type { HashPriority } from '../StyleContext';
import type { ExtractStyle } from './useGlobalCache';
declare const SKIP_CHECK = "_skip_check_";
declare const MULTI_VALUE = "_multi_value_";
export type CSSProperties = Omit<CSS.PropertiesFallback<number | string>, 'animationName'> & {
    animationName?: CSS.PropertiesFallback<number | string>['animationName'] | Keyframes;
};
export type CSSPropertiesWithMultiValues = {
    [K in keyof CSSProperties]: CSSProperties[K] | readonly Extract<CSSProperties[K], string>[] | {
        [SKIP_CHECK]?: boolean;
        [MULTI_VALUE]?: boolean;
        value: CSSProperties[K] | CSSProperties[K][];
    };
};
export type CSSPseudos = {
    [K in CSS.Pseudos]?: CSSObject;
};
type ArrayCSSInterpolation = readonly CSSInterpolation[];
export type InterpolationPrimitive = null | undefined | boolean | number | string | CSSObject;
export type CSSInterpolation = InterpolationPrimitive | ArrayCSSInterpolation | Keyframes;
export type CSSOthersObject = Record<string, CSSInterpolation>;
export interface CSSObject extends CSSPropertiesWithMultiValues, CSSPseudos, CSSOthersObject {
}
export declare function normalizeStyle(styleStr: string): string;
export interface ParseConfig {
    hashId?: string;
    hashPriority?: HashPriority;
    layer?: string;
    path?: string;
    transformers?: Transformer[];
    linters?: Linter[];
}
export interface ParseInfo {
    root?: boolean;
    injectHash?: boolean;
    parentSelectors: string[];
}
export declare const parseStyle: (interpolation: CSSInterpolation, config?: ParseConfig, { root, injectHash, parentSelectors }?: ParseInfo) => [parsedStr: string, effectStyle: Record<string, string>];
export declare function uniqueHash(path: (string | number)[], styleStr: string): string;
export declare const STYLE_PREFIX = "style";
type StyleCacheValue = [
    styleStr: string,
    tokenKey: string,
    styleId: string,
    effectStyle: Record<string, string>,
    clientOnly: boolean | undefined,
    order: number
];
/**
 * Register a style to the global style sheet.
 */
export default function useStyleRegister(info: {
    theme: Theme<any, any>;
    token: any;
    path: string[];
    hashId?: string;
    layer?: string;
    nonce?: string | (() => string);
    clientOnly?: boolean;
    /**
     * Tell cssinjs the insert order of style.
     * It's useful when you need to insert style
     * before other style to overwrite for the same selector priority.
     */
    order?: number;
}, styleFn: () => CSSInterpolation): (node: React.ReactElement) => React.JSX.Element;
export declare const extract: ExtractStyle<StyleCacheValue>;
export {};
