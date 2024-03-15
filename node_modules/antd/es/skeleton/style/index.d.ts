/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export type ComponentToken = {
    /** @deprecated use gradientFromColor instead. */
    color: string;
    /** @deprecated use gradientToColor instead. */
    colorGradientEnd: string;
    /**
     * @desc 渐变色起点颜色
     * @descEN Start color of gradient
     */
    gradientFromColor: string;
    /**
     * @desc 渐变色终点颜色
     * @descEN End color of gradient
     */
    gradientToColor: string;
    /**
     * @desc 标题骨架屏高度
     * @descEN Height of title skeleton
     */
    titleHeight: number;
    /**
     * @desc 骨架屏圆角
     * @descEN Border radius of skeleton
     */
    blockRadius: number;
    /**
     * @desc 段落骨架屏上间距
     * @descEN Margin top of paragraph skeleton
     */
    paragraphMarginTop: number;
    /**
     * @desc 段落骨架屏单行高度
     * @descEN Line height of paragraph skeleton
     */
    paragraphLiHeight: number;
};
export declare const prepareComponentToken: GetDefaultToken<'Skeleton'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
