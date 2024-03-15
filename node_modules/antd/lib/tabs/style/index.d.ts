/// <reference types="react" />
import type { FullToken, GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 下拉菜单 z-index
     * @descEN z-index of dropdown menu
     */
    zIndexPopup: number;
    /**
     * @desc 卡片标签页背景色
     * @descEN Background color of card tab
     */
    cardBg: string;
    /**
     * @desc 卡片标签页高度
     * @descEN Height of card tab
     */
    cardHeight: number;
    /**
     * @desc 卡片标签页内间距
     * @descEN Padding of card tab
     */
    cardPadding: string;
    /**
     * @desc 小号卡片标签页内间距
     * @descEN Padding of small card tab
     */
    cardPaddingSM: string;
    /**
     * @desc 大号卡片标签页内间距
     * @descEN Padding of large card tab
     */
    cardPaddingLG: string;
    /**
     * @desc 标齐页标题文本大小
     * @descEN Font size of title
     */
    titleFontSize: number;
    /**
     * @desc 大号标签页标题文本大小
     * @descEN Font size of large title
     */
    titleFontSizeLG: number;
    /**
     * @desc 小号标签页标题文本大小
     * @descEN Font size of small title
     */
    titleFontSizeSM: number;
    /**
     * @desc 指示条颜色
     * @descEN Color of indicator
     */
    inkBarColor: string;
    /**
     * @desc 横向标签页外间距
     * @descEN Horizontal margin of horizontal tab
     */
    horizontalMargin: string;
    /**
     * @desc 横向标签页标签间距
     * @descEN Horizontal gutter of horizontal tab
     */
    horizontalItemGutter: number;
    /**
     * @desc 横向标签页标签外间距
     * @descEN Horizontal margin of horizontal tab item
     */
    horizontalItemMargin: string;
    /**
     * @desc 横向标签页标签外间距（RTL）
     * @descEN Horizontal margin of horizontal tab item (RTL)
     */
    horizontalItemMarginRTL: string;
    /**
     * @desc 横向标签页标签内间距
     * @descEN Horizontal padding of horizontal tab item
     */
    horizontalItemPadding: string;
    /**
     * @desc 大号横向标签页标签内间距
     * @descEN Horizontal padding of large horizontal tab item
     */
    horizontalItemPaddingLG: string;
    /**
     * @desc 小号横向标签页标签内间距
     * @descEN Horizontal padding of small horizontal tab item
     */
    horizontalItemPaddingSM: string;
    /**
     * @desc 纵向标签页标签内间距
     * @descEN Vertical padding of vertical tab item
     */
    verticalItemPadding: string;
    /**
     * @desc 纵向标签页标签外间距
     * @descEN Vertical margin of vertical tab item
     */
    verticalItemMargin: string;
    /**
     * @desc 标签文本颜色
     * @descEN Text color of tab
     */
    itemColor: string;
    /**
     * @desc 标签激活态文本颜色
     * @descEN Text color of active tab
     */
    itemActiveColor: string;
    /**
     * @desc 标签悬浮态文本颜色
     * @descEN Text color of hover tab
     */
    itemHoverColor: string;
    /**
     * @desc 标签选中态文本颜色
     * @descEN Text color of selected tab
     */
    itemSelectedColor: string;
    /**
     * @desc 卡片标签间距
     * @descEN Gutter of card tab
     */
    cardGutter: number;
}
export interface TabsToken extends FullToken<'Tabs'> {
    tabsCardPadding: string;
    dropdownEdgeChildVerticalPadding: number;
    tabsNavWrapPseudoWidth: number;
    tabsActiveTextShadow: string;
    tabsDropdownHeight: number;
    tabsDropdownWidth: number;
    tabsHorizontalItemMargin: string;
    tabsHorizontalItemMarginRTL: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Tabs'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
