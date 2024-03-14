import type { CSSProperties } from 'react';
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 标题字体大小
     * @descEN Title font size
     */
    titleFontSize: number;
    /**
     * @desc 副标题字体大小
     * @descEN Subtitle font size
     */
    subtitleFontSize: number;
    /**
     * @desc 图标大小
     * @descEN Icon size
     */
    iconFontSize: number;
    /**
     * @desc 额外区域外间距
     * @descEN Margin of extra area
     */
    extraMargin: CSSProperties['margin'];
}
export declare const prepareComponentToken: GetDefaultToken<'Result'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
