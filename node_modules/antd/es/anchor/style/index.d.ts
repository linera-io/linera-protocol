/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 链接纵向内间距
     * @descEN Vertical padding of link
     */
    linkPaddingBlock: number;
    /**
     * @desc 链接横向内间距
     * @descEN Horizontal padding of link
     */
    linkPaddingInlineStart: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Anchor'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
