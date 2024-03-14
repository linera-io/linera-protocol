/// <reference types="react" />
import type { ArrowOffsetToken } from '../../style/placementArrow';
import type { FullToken, GetDefaultToken } from '../../theme/internal';
import type { ArrowToken } from '../../style/roundedArrow';
export interface ComponentToken extends ArrowToken, ArrowOffsetToken {
    /**
     * @deprecated Please use `titleMinWidth` instead
     * @desc 气泡卡片宽度
     * @descEN Width of Popover
     */
    width?: number;
    /**
     * @deprecated Please use `titleMinWidth` instead
     * @desc 气泡卡片最小宽度
     * @descEN Min width of Popover
     */
    minWidth?: number;
    /**
     * @desc 气泡卡片标题最小宽度
     * @descEN Min width of Popover title
     */
    titleMinWidth: number;
    /**
     * @desc 气泡卡片 z-index
     * @descEN z-index of Popover
     */
    zIndexPopup: number;
}
export type PopoverToken = FullToken<'Popover'> & {
    popoverBg: string;
    popoverColor: string;
};
export declare const prepareComponentToken: GetDefaultToken<'Popover'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
