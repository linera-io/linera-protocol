/// <reference types="react" />
import type { FullToken, GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 确认框 z-index
     * @descEN z-index of Popconfirm
     */
    zIndexPopup: number;
}
export interface PopconfirmToken extends FullToken<'Popconfirm'> {
}
export declare const prepareComponentToken: GetDefaultToken<'Popconfirm'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
