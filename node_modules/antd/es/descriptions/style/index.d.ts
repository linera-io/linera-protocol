/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 标签背景色
     * @descEN Background color of label
     */
    labelBg: string;
    /**
     * @desc 标题文字颜色
     * @descEN Text color of title
     */
    titleColor: string;
    /**
     * @desc 标题下间距
     * @descEN Bottom margin of title
     */
    titleMarginBottom: number;
    /**
     * @desc 子项下间距
     * @descEN Bottom padding of item
     */
    itemPaddingBottom: number;
    /**
     * @desc 冒号右间距
     * @descEN Right margin of colon
     */
    colonMarginRight: number;
    /**
     * @desc 冒号左间距
     * @descEN Left margin of colon
     */
    colonMarginLeft: number;
    /**
     * @desc 内容区域文字颜色
     * @descEN Text color of content
     */
    contentColor: string;
    /**
     * @desc 额外区域文字颜色
     * @descEN Text color of extra area
     */
    extraColor: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Descriptions'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
