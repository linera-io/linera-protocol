import * as React from 'react';
export declare function noop(): void;
export declare function resetWarned(): void;
type Warning = (valid: boolean, component: string, message?: string) => void;
declare let warning: Warning;
type BaseTypeWarning = (valid: boolean, 
/**
 * - deprecated: Some API will be removed in future but still support now.
 * - usage: Some API usage is not correct.
 * - breaking: Breaking change like API is removed.
 */
type: 'deprecated' | 'usage' | 'breaking', message?: string) => void;
type TypeWarning = BaseTypeWarning & {
    deprecated: (valid: boolean, oldProp: string, newProp: string, message?: string) => void;
};
export interface WarningContextProps {
    strict?: boolean;
}
export declare const WarningContext: React.Context<WarningContextProps>;
/**
 * This is a hook but we not named as `useWarning`
 * since this is only used in development.
 * We should always wrap this in `if (process.env.NODE_ENV !== 'production')` condition
 */
export declare const devUseWarning: (component: string) => TypeWarning;
export default warning;
