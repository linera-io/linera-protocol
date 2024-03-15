import React from 'react';
import type { Theme } from '@ant-design/cssinjs';
import type { AliasToken, MapToken, OverrideToken, SeedToken } from './interface';
export declare const defaultTheme: Theme<any, any>;
export declare const defaultConfig: {
    token: SeedToken;
    override: {
        override: SeedToken;
    };
    hashed: boolean;
};
export type ComponentsToken = {
    [key in keyof OverrideToken]?: OverrideToken[key] & {
        theme?: Theme<SeedToken, MapToken>;
    };
};
export interface DesignTokenProviderProps {
    token: Partial<AliasToken>;
    theme?: Theme<SeedToken, MapToken>;
    components?: ComponentsToken;
    /** Just merge `token` & `override` at top to save perf */
    override: {
        override: Partial<AliasToken>;
    } & ComponentsToken;
    hashed?: string | boolean;
    cssVar?: {
        prefix?: string;
        key?: string;
    };
}
export declare const DesignTokenContext: React.Context<DesignTokenProviderProps>;
