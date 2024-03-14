import type React from 'react';
import type { GlobalToken } from '../../theme';
import type { AliasToken, FullToken, GenerateStyle } from '../../theme/internal';
import type { GenStyleFn, TokenWithCommonCls } from '../../theme/util/genComponentStyleHook';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 顶部背景色
     * @descEN Background color of header
     */
    headerBg: string;
    /**
     * @desc 标题行高
     * @descEN Line height of title
     */
    titleLineHeight: number;
    /**
     * @desc 标题字体大小
     * @descEN Font size of title
     */
    titleFontSize: number;
    /**
     * @desc 标题字体颜色
     * @descEN Font color of title
     */
    titleColor: string;
    /**
     * @desc 内容区域背景色
     * @descEN Background color of content
     */
    contentBg: string;
    /**
     * @desc 底部区域背景色
     * @descEN Background color of footer
     */
    footerBg: string;
}
export interface ModalToken extends FullToken<'Modal'> {
    modalHeaderHeight: number | string;
    modalFooterBorderColorSplit: string;
    modalFooterBorderStyle: string;
    modalFooterBorderWidth: number;
    modalIconHoverColor: string;
    modalCloseIconColor: string;
    modalCloseBtnSize: number | string;
    modalConfirmIconSize: number | string;
    modalTitleHeight: number | string;
}
export declare const genModalMaskStyle: GenerateStyle<TokenWithCommonCls<AliasToken>>;
export declare const prepareToken: (token: Parameters<GenStyleFn<'Modal'>>[0]) => ModalToken;
export declare const prepareComponentToken: (token: GlobalToken) => {
    footerBg: string;
    headerBg: string;
    titleLineHeight: number;
    titleFontSize: number;
    contentBg: string;
    titleColor: string;
    closeBtnHoverBg: string;
    closeBtnActiveBg: string;
    contentPadding: string | number;
    headerPadding: string | number;
    headerBorderBottom: string;
    headerMarginBottom: number;
    bodyPadding: number;
    footerPadding: string | number;
    footerBorderTop: string;
    footerBorderRadius: string | number;
    footerMarginTop: number;
    confirmBodyPadding: string | number;
    confirmIconMarginInlineEnd: number;
    confirmBtnsMarginTop: number;
};
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: React.ReactElement<any, string | React.JSXElementConstructor<any>>) => React.ReactElement<any, string | React.JSXElementConstructor<any>>, string, string | undefined];
export default _default;
