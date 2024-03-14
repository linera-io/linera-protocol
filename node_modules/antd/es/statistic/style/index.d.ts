/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 标题字体大小
     * @descEN Title font size
     */
    titleFontSize: number;
    /**
     * @desc 内容字体大小
     * @descEN Content font size
     */
    contentFontSize: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Statistic'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
