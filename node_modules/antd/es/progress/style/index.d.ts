/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 进度条默认颜色
     * @descEN Default color of progress bar
     */
    defaultColor: string;
    /**
     * @desc 进度条剩余部分颜色
     * @descEN Color of remaining part of progress bar
     */
    remainingColor: string;
    /**
     * @desc 圆形进度条文字颜色
     * @descEN Text color of circular progress bar
     */
    circleTextColor: string;
    /**
     * @desc 条状进度条圆角
     * @descEN Border radius of line progress bar
     */
    lineBorderRadius: number;
    /**
     * @desc 圆形进度条文本大小
     * @descEN Text size of circular progress bar
     */
    circleTextFontSize: string;
    /**
     * @desc 圆形进度条图标大小
     * @descEN Icon size of circular progress bar
     */
    circleIconFontSize: string;
}
export declare const LineStrokeColorVar = "--progress-line-stroke-color";
export declare const Percent = "--progress-percent";
export declare const prepareComponentToken: GetDefaultToken<'Progress'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
