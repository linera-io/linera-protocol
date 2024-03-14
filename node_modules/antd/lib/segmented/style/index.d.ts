/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 选项文本颜色
     * @descEN Text color of item
     */
    itemColor: string;
    /**
     * @desc 选项悬浮态文本颜色
     * @descEN Text color of item when hover
     */
    itemHoverColor: string;
    /**
     * @desc 选项悬浮态背景颜色
     * @descEN Background color of item when hover
     */
    itemHoverBg: string;
    /**
     * @desc 选项激活态背景颜色
     * @descEN Background color of item when active
     */
    itemActiveBg: string;
    /**
     * @desc 选项选中时背景颜色
     * @descEN Background color of item when selected
     */
    itemSelectedBg: string;
    /**
     * @desc 选项选中时文字颜色
     * @descEN Text color of item when selected
     */
    itemSelectedColor: string;
    /**
     * @desc Segmented 控件容器的 padding
     * @descEN Padding of Segmented container
     */
    trackPadding: string | number;
    /**
     * @desc Segmented 控件容器背景色
     * @descEN Background of Segmented container
     */
    trackBg: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Segmented'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
