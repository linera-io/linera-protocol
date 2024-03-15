/// <reference types="react" />
import type { CSSObject } from '@ant-design/cssinjs';
import type { ComponentToken, InputToken } from './token';
import { initComponentToken, initInputToken } from './token';
export type { ComponentToken };
export { initComponentToken, initInputToken };
export declare const genPlaceholderStyle: (color: string) => CSSObject;
export declare const genActiveStyle: (token: InputToken) => {
    borderColor: string;
    boxShadow: string;
    outline: number;
    backgroundColor: string;
};
export declare const genInputSmallStyle: (token: InputToken) => CSSObject;
export declare const genBasicInputStyle: (token: InputToken) => CSSObject;
export declare const genInputGroupStyle: (token: InputToken) => CSSObject;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
