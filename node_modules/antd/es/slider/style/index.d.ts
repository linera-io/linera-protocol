import type * as React from 'react';
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 滑动输入高度
     * @descEN Height of slider
     */
    controlSize: number;
    /**
     * @desc 轨道高度
     * @descEN Height of rail
     */
    railSize: number;
    /**
     * @desc 滑块尺寸
     * @descEN Size of handle
     */
    handleSize: number;
    /**
     * @desc 滑块尺寸（悬浮态）
     * @descEN Size of handle when hover
     */
    handleSizeHover: number;
    /**
     * @desc 滑块边框宽度
     * @descEN Border width of handle
     */
    handleLineWidth: number;
    /**
     * @desc 滑块边框宽度（悬浮态）
     * @descEN Border width of handle when hover
     */
    handleLineWidthHover: number;
    /**
     * @desc 滑块圆点尺寸
     * @descEN Size of dot
     */
    dotSize: number;
    /**
     * @desc 轨道背景色
     * @descEN Background color of rail
     */
    railBg: string;
    /**
     * @desc 轨道背景色（悬浮态）
     * @descEN Background color of rail when hover
     */
    railHoverBg: string;
    /**
     * @desc 轨道已覆盖部分背景色
     * @descEN Background color of track
     */
    trackBg: string;
    /**
     * @desc 轨道已覆盖部分背景色（悬浮态）
     * @descEN Background color of track when hover
     */
    trackHoverBg: string;
    /**
     * @desc 滑块颜色
     * @descEN Color of handle
     */
    handleColor: string;
    /**
     * @desc 滑块激活态颜色
     * @descEN Color of handle when active
     */
    handleActiveColor: string;
    /**
     * @desc 滑块禁用颜色
     * @descEN Color of handle when disabled
     */
    handleColorDisabled: string;
    /**
     * @desc 圆点边框颜色
     * @descEN Border color of dot
     */
    dotBorderColor: string;
    /**
     * @desc 圆点激活态边框颜色
     * @descEN Border color of dot when active
     */
    dotActiveBorderColor: string;
    /**
     * @desc 轨道禁用态背景色
     * @descEN Background color of track when disabled
     */
    trackBgDisabled: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Slider'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: React.ReactElement<any, string | React.JSXElementConstructor<any>>) => React.ReactElement<any, string | React.JSXElementConstructor<any>>, string, string | undefined];
export default _default;
