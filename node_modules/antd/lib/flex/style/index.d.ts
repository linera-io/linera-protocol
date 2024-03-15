/// <reference types="react" />
import type { FullToken, GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
}
export interface FlexToken extends FullToken<'Flex'> {
    /**
     * @nameZH 小间隙
     * @nameEN Small Gap
     * @desc 控制元素的小间隙。
     * @descEN Control the small gap of the element.
     */
    flexGapSM: number;
    /**
     * @nameZH 间隙
     * @nameEN Gap
     * @desc 控制元素的间隙。
     * @descEN Control the gap of the element.
     */
    flexGap: number;
    /**
     * @nameZH 大间隙
     * @nameEN Large Gap
     * @desc 控制元素的大间隙。
     * @descEN Control the large gap of the element.
     */
    flexGapLG: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Flex'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
