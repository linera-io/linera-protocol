import type { ArrowOffsetToken } from '../../style/placementArrow';
import type { FullToken, GetDefaultToken } from '../../theme/internal';
import type { ArrowToken } from '../../style/roundedArrow';
import type { CSSProperties } from 'react';
export interface ComponentToken extends ArrowToken, ArrowOffsetToken {
    /**
     * @desc 下拉菜单 z-index
     * @descEN z-index of dropdown
     */
    zIndexPopup: number;
    /**
     * @desc 下拉菜单纵向内边距
     * @descEN Vertical padding of dropdown
     */
    paddingBlock: CSSProperties['paddingBlock'];
}
export interface DropdownToken extends FullToken<'Dropdown'> {
    dropdownArrowDistance: number | string;
    dropdownEdgeChildPadding: number;
    menuCls: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Dropdown'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
