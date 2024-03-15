/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 卡片头部背景色
     * @descEN Background color of card header
     */
    headerBg: string;
    /**
     * @desc 卡片头部文字大小
     * @descEN Font size of card header
     */
    headerFontSize: number;
    /**
     * @desc 小号卡片头部文字大小
     * @descEN Font size of small card header
     */
    headerFontSizeSM: number;
    /**
     * @desc 卡片头部高度
     * @descEN Height of card header
     */
    headerHeight: number;
    /**
     * @desc 小号卡片头部高度
     * @descEN Height of small card header
     */
    headerHeightSM: number;
    /**
     * @desc 操作区背景色
     * @descEN Background color of card actions
     */
    actionsBg: string;
    /**
     * @desc 操作区每一项的外间距
     * @descEN Margin of each item in card actions
     */
    actionsLiMargin: string;
    /**
     * @desc 内置标签页组件下间距
     * @descEN Margin bottom of tabs component
     */
    tabsMarginBottom: number;
    /**
     * @desc 额外区文字颜色
     * @descEN Text color of extra area
     */
    extraColor: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Card'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
