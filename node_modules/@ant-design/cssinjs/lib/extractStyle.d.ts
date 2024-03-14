import type Cache from './Cache';
declare const ExtractStyleFns: {
    style: import("./hooks/useGlobalCache").ExtractStyle<[styleStr: string, tokenKey: string, styleId: string, effectStyle: Record<string, string>, clientOnly: boolean | undefined, order: number]>;
    token: import("./hooks/useGlobalCache").ExtractStyle<[token: any, hashId: string, realToken: any, cssVarStr: string, cssVarKey: string]>;
    cssVar: import("./hooks/useGlobalCache").ExtractStyle<[cssVarToken: import("./util/css-variables").TokenWithCSSVar<any, Record<string, any>>, cssVarStr: string, styleId: string, cssVarKey: string]>;
};
type ExtractStyleType = keyof typeof ExtractStyleFns;
export default function extractStyle(cache: Cache, options?: boolean | {
    plain?: boolean;
    types?: ExtractStyleType | ExtractStyleType[];
}): string;
export {};
