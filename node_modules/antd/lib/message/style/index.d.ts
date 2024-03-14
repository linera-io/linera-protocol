import type { CSSProperties } from 'react';
import type { GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 提示框 z-index
     * @descEN z-index of Message
     */
    zIndexPopup: number;
    /**
     * @desc 提示框背景色
     * @descEN Background color of Message
     */
    contentBg: string;
    /**
     * @desc 提示框内边距
     * @descEN Padding of Message
     */
    contentPadding: CSSProperties['padding'];
}
export declare const prepareComponentToken: GetDefaultToken<'Message'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
