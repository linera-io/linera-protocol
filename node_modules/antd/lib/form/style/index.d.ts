import type { CSSProperties } from 'react';
import type { FullToken, GetDefaultToken } from '../../theme/internal';
import type { GenStyleFn } from '../../theme/util/genComponentStyleHook';
export interface ComponentToken {
    /**
     * @desc 必填项标记颜色
     * @descEN Required mark color
     */
    labelRequiredMarkColor: string;
    /**
     * @desc 标签颜色
     * @descEN Label color
     */
    labelColor: string;
    /**
     * @desc 标签字体大小
     * @descEN Label font size
     */
    labelFontSize: number;
    /**
     * @desc 标签高度
     * @descEN Label height
     */
    labelHeight: number;
    /**
     * @desc 标签冒号前间距
     * @descEN Label colon margin-inline-start
     */
    labelColonMarginInlineStart: number;
    /**
     * @desc 标签冒号后间距
     * @descEN Label colon margin-inline-end
     */
    labelColonMarginInlineEnd: number;
    /**
     * @desc 表单项间距
     * @descEN Form item margin bottom
     */
    itemMarginBottom: number;
    /**
     * @desc 垂直布局标签内边距
     * @descEN Vertical layout label padding
     */
    verticalLabelPadding: CSSProperties['padding'];
    /**
     * @desc 垂直布局标签外边距
     * @descEN Vertical layout label margin
     */
    verticalLabelMargin: CSSProperties['margin'];
}
export interface FormToken extends FullToken<'Form'> {
    formItemCls: string;
    rootPrefixCls: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Form'>;
export declare const prepareToken: (token: Parameters<GenStyleFn<'Form'>>[0], rootPrefixCls: string) => FormToken;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
