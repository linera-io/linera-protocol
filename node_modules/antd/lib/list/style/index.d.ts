import type { CSSProperties } from 'react';
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 内容宽度
     * @descEN Width of content
     */
    contentWidth: number;
    /**
     * @desc 大号列表项内间距
     * @descEN Padding of large item
     */
    itemPaddingLG: string;
    /**
     * @desc 小号列表项内间距
     * @descEN Padding of small item
     */
    itemPaddingSM: string;
    /**
     * @desc 列表项内间距
     * @descEN Padding of item
     */
    itemPadding: string;
    /**
     * @desc 头部区域背景色
     * @descEN Background color of header
     */
    headerBg: string;
    /**
     * @desc 底部区域背景色
     * @descEN Background color of footer
     */
    footerBg: string;
    /**
     * @desc 空文本内边距
     * @descEN Padding of empty text
     */
    emptyTextPadding: CSSProperties['padding'];
    /**
     * @desc Meta 下间距
     * @descEN Margin bottom of meta
     */
    metaMarginBottom: CSSProperties['marginBottom'];
    /**
     * @desc 头像右间距
     * @descEN Right margin of avatar
     */
    avatarMarginRight: CSSProperties['marginRight'];
    /**
     * @desc 标题下间距
     * @descEN Margin bottom of title
     */
    titleMarginBottom: CSSProperties['marginBottom'];
    /**
     * @desc 描述文字大小
     * @descEN Font size of description
     */
    descriptionFontSize: number;
}
export declare const prepareComponentToken: GetDefaultToken<'List'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
