/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 指示点宽度
     * @descEN Width of indicator
     */
    dotWidth: number;
    /**
     * @desc 指示点高度
     * @descEN Height of indicator
     */
    dotHeight: number;
    /** @deprecated Use `dotActiveWidth` instead. */
    dotWidthActive: number;
    /**
     * @desc 激活态指示点宽度
     * @descEN Width of active indicator
     */
    dotActiveWidth: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Carousel'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
