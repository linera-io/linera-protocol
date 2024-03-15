/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     *  @desc 开关高度
     *  @descEN Height of Switch
     */
    trackHeight: number;
    /**
     * @desc 小号开关高度
     * @descEN Height of small Switch
     */
    trackHeightSM: number;
    /**
     * @desc 开关最小宽度
     * @descEN Minimum width of Switch
     */
    trackMinWidth: number;
    /**
     * @desc 小号开关最小宽度
     * @descEN Minimum width of small Switch
     */
    trackMinWidthSM: number;
    /**
     * @desc 开关内边距
     * @descEN Padding of Switch
     */
    trackPadding: number;
    /**
     * @desc 开关把手背景色
     * @descEN Background color of Switch handle
     */
    handleBg: string;
    /**
     * @desc 开关把手阴影
     * @descEN Shadow of Switch handle
     */
    handleShadow: string;
    /**
     * @desc 开关把手大小
     * @descEN Size of Switch handle
     */
    handleSize: number;
    /**
     * @desc 小号开关把手大小
     * @descEN Size of small Switch handle
     */
    handleSizeSM: number;
    /**
     * @desc 内容区域最小边距
     * @descEN Minimum margin of content area
     */
    innerMinMargin: number;
    /**
     * @desc 内容区域最大边距
     * @descEN Maximum margin of content area
     */
    innerMaxMargin: number;
    /**
     * @desc 小号开关内容区域最小边距
     * @descEN Minimum margin of content area of small Switch
     */
    innerMinMarginSM: number;
    /**
     * @desc 小号开关内容区域最大边距
     * @descEN Maximum margin of content area of small Switch
     */
    innerMaxMarginSM: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Switch'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
