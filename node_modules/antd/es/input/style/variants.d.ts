import type { CSSObject } from '@ant-design/cssinjs';
import type { InputToken } from './token';
export declare const genHoverStyle: (token: InputToken) => CSSObject;
export declare const genDisabledStyle: (token: InputToken) => CSSObject;
export declare const genBaseOutlinedStyle: (token: InputToken, options: {
    borderColor: string;
    hoverBorderColor: string;
    activeBorderColor: string;
    activeShadow: string;
}) => CSSObject;
export declare const genOutlinedStyle: (token: InputToken, extraStyles?: CSSObject) => CSSObject;
export declare const genOutlinedGroupStyle: (token: InputToken) => CSSObject;
export declare const genBorderlessStyle: (token: InputToken, extraStyles?: CSSObject) => CSSObject;
export declare const genFilledStyle: (token: InputToken, extraStyles?: CSSObject) => CSSObject;
export declare const genFilledGroupStyle: (token: InputToken) => CSSObject;
