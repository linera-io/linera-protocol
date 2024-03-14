import type { CSSProperties } from 'react';
import type { FullToken, GenerateStyle, GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 折叠面板头部内边距
     * @descEN Padding of header
     */
    headerPadding: CSSProperties['padding'];
    /**
     * @desc 折叠面板头部背景
     * @descEN Background of header
     */
    headerBg: string;
    /**
     * @desc 折叠面板内容内部编辑
     * @descEN Padding of content
     */
    contentPadding: CSSProperties['padding'];
    /**
     * @desc 折叠面板内容背景
     * @descEN Background of content
     */
    contentBg: string;
}
type CollapseToken = FullToken<'Collapse'> & {
    collapseHeaderPaddingSM: string;
    collapseHeaderPaddingLG: string;
    collapsePanelBorderRadius: number;
};
export declare const genBaseStyle: GenerateStyle<CollapseToken>;
export declare const prepareComponentToken: GetDefaultToken<'Collapse'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
