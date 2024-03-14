/// <reference types="react" />
import type { FullToken, GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 标题上间距
     * @descEN Margin top of title
     */
    titleMarginTop: number | string;
    /**
     * @desc 标题下间距
     * @descEN Margin bottom of title
     */
    titleMarginBottom: number | string;
}
export type TypographyToken = FullToken<'Typography'>;
export declare const prepareComponentToken: GetDefaultToken<'Typography'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
