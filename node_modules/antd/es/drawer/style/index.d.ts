/// <reference types="react" />
import type { FullToken, GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 弹窗 z-index
     * @descEN z-index of drawer
     */
    zIndexPopup: number;
    /**
     * @desc 底部区域纵向内间距
     * @descEN Vertical padding of footer
     */
    footerPaddingBlock: number;
    /**
     * @desc 底部区域横向内间距
     * @descEN Horizontal padding of footer
     */
    footerPaddingInline: number;
}
export interface DrawerToken extends FullToken<'Drawer'> {
}
export declare const prepareComponentToken: GetDefaultToken<'Drawer'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
