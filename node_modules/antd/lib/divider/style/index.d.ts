import type { CSSProperties } from 'react';
import type { GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 文本横向内间距
     * @descEN Horizontal padding of text
     */
    textPaddingInline: CSSProperties['paddingInline'];
    /**
     * @desc 文本与边缘距离，取值 0 ～ 1
     * @descEN Distance between text and edge, which should be a number between 0 and 1.
     */
    orientationMargin: number;
    /**
     * @desc 纵向分割线的横向外间距
     * @descEN Horizontal margin of vertical Divider
     */
    verticalMarginInline: CSSProperties['marginInline'];
}
export declare const prepareComponentToken: GetDefaultToken<'Divider'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
