import type { CSSProperties } from 'react';
import type { GlobalToken } from '../../theme';
import type { FullToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 选择器宽度
     * @descEN Width of Cascader
     */
    controlWidth: number;
    /**
     * @desc 选项宽度
     * @descEN Width of item
     */
    controlItemWidth: number;
    /**
     * @desc 下拉菜单高度
     * @descEN Height of dropdown
     */
    dropdownHeight: number;
    /**
     * @desc 选项选中时背景色
     * @descEN Background color of selected item
     */
    optionSelectedBg: string;
    /**
     * @desc 选项选中时字重
     * @descEN Font weight of selected item
     */
    optionSelectedFontWeight: CSSProperties['fontWeight'];
    /**
     * @desc 选项内间距
     * @descEN Padding of menu item
     */
    optionPadding: CSSProperties['padding'];
    /**
     * @desc 选项菜单（单列）内间距
     * @descEN Padding of menu item (single column)
     */
    menuPadding: CSSProperties['padding'];
}
export type CascaderToken = FullToken<'Cascader'>;
export declare const prepareComponentToken: (token: GlobalToken) => {
    controlWidth: number;
    controlItemWidth: number;
    dropdownHeight: number;
    optionSelectedBg: string;
    optionSelectedFontWeight: number;
    optionPadding: string;
    menuPadding: number;
};
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
