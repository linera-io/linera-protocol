/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 单选框大小
     * @descEN Radio size
     */
    radioSize: number;
    /**
     * @desc 单选框圆点大小
     * @descEN Size of Radio dot
     */
    dotSize: number;
    /**
     * @desc 单选框圆点禁用颜色
     * @descEN Color of disabled Radio dot
     */
    dotColorDisabled: string;
    /**
     * @desc 单选框按钮背景色
     * @descEN Background color of Radio button
     */
    buttonBg: string;
    /**
     * @desc 单选框按钮选中背景色
     * @descEN Background color of checked Radio button
     */
    buttonCheckedBg: string;
    /**
     * @desc 单选框按钮文本颜色
     * @descEN Color of Radio button text
     */
    buttonColor: string;
    /**
     * @desc 单选框按钮横向内间距
     * @descEN Horizontal padding of Radio button
     */
    buttonPaddingInline: number;
    /**
     * @desc 单选框按钮选中并禁用时的背景色
     * @descEN Background color of checked and disabled Radio button
     */
    buttonCheckedBgDisabled: string;
    /**
     * @desc 单选框按钮选中并禁用时的文本颜色
     * @descEN Color of checked and disabled Radio button text
     */
    buttonCheckedColorDisabled: string;
    /**
     * @desc 单选框实色按钮选中时的文本颜色
     * @descEN Color of checked solid Radio button text
     */
    buttonSolidCheckedColor: string;
    /**
     * @desc 单选框实色按钮选中时的背景色
     * @descEN Background color of checked solid Radio button text
     */
    buttonSolidCheckedBg: string;
    /**
     * @desc 单选框实色按钮选中时的悬浮态背景色
     * @descEN Background color of checked solid Radio button text when hover
     */
    buttonSolidCheckedHoverBg: string;
    /**
     * @desc 单选框实色按钮选中时的激活态背景色
     * @descEN Background color of checked solid Radio button text when active
     */
    buttonSolidCheckedActiveBg: string;
    /**
     * @desc 单选框右间距
     * @descEN Margin right of Radio button
     */
    wrapperMarginInlineEnd: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Radio'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
