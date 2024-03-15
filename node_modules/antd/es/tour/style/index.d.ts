/// <reference types="react" />
import type { ArrowOffsetToken } from '../../style/placementArrow';
import type { GetDefaultToken } from '../../theme/internal';
import type { ArrowToken } from '../../style/roundedArrow';
export interface ComponentToken extends ArrowOffsetToken, ArrowToken {
    /**
     * @desc 弹层 z-index
     * @descEN Tour popup z-index
     */
    zIndexPopup: number;
    /**
     * @desc 关闭按钮尺寸
     * @descEN Close button size
     */
    closeBtnSize: number;
    /**
     * @desc Primary 模式上一步按钮背景色
     * @descEN Background color of previous button in primary type
     */
    primaryPrevBtnBg: string;
    /**
     * @desc Primary 模式下一步按钮悬浮背景色
     * @descEN Hover background color of next button in primary type
     */
    primaryNextBtnHoverBg: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Tour'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
