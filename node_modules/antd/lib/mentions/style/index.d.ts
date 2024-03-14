/// <reference types="react" />
import type { SharedComponentToken } from '../../input/style/token';
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken extends SharedComponentToken {
    /**
     * @desc 弹层 z-index
     * @descEN z-index of popup
     */
    zIndexPopup: number;
    /**
     * @desc 弹层高度
     * @descEN Height of popup
     */
    dropdownHeight: number;
    /**
     * @desc 菜单项高度
     * @descEN Height of menu item
     */
    controlItemWidth: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Mentions'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
