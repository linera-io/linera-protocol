import * as React from 'react';
import CacheEntity from './Cache';
import type { Linter } from './linters/interface';
import type { Transformer } from './transformers/interface';
export declare const ATTR_TOKEN = "data-token-hash";
export declare const ATTR_MARK = "data-css-hash";
export declare const ATTR_CACHE_PATH = "data-cache-path";
export declare const CSS_IN_JS_INSTANCE = "__cssinjs_instance__";
export declare function createCache(): CacheEntity;
export type HashPriority = 'low' | 'high';
export interface StyleContextProps {
    autoClear?: boolean;
    /** @private Test only. Not work in production. */
    mock?: 'server' | 'client';
    /**
     * Only set when you need ssr to extract style on you own.
     * If not provided, it will auto create <style /> on the end of Provider in server side.
     */
    cache: CacheEntity;
    /** Tell children that this context is default generated context */
    defaultCache: boolean;
    /** Use `:where` selector to reduce hashId css selector priority */
    hashPriority?: HashPriority;
    /** Tell cssinjs where to inject style in */
    container?: Element | ShadowRoot;
    /** Component wil render inline  `<style />` for fallback in SSR. Not recommend. */
    ssrInline?: boolean;
    /** Transform css before inject in document. Please note that `transformers` do not support dynamic update */
    transformers?: Transformer[];
    /**
     * Linters to lint css before inject in document.
     * Styles will be linted after transforming.
     * Please note that `linters` do not support dynamic update.
     */
    linters?: Linter[];
}
declare const StyleContext: React.Context<StyleContextProps>;
export type StyleProviderProps = Partial<StyleContextProps> & {
    children?: React.ReactNode;
};
export declare const StyleProvider: React.FC<StyleProviderProps>;
export default StyleContext;
