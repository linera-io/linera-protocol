/// <reference types="react" />
import type { CSSObject } from '@ant-design/cssinjs';
import type { AliasToken, FullToken } from '../../theme/internal';
import type { GenStyleFn } from '../../theme/util/genComponentStyleHook';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 提醒框 z-index
     * @descEN z-index of Notification
     */
    zIndexPopup: number;
    /**
     * @desc 提醒框宽度
     * @descEN Width of Notification
     */
    width: number;
}
export interface NotificationToken extends FullToken<'Notification'> {
    animationMaxHeight: number;
    notificationBg: string;
    notificationPadding: string;
    notificationPaddingVertical: number;
    notificationPaddingHorizontal: number;
    notificationIconSize: number | string;
    notificationCloseButtonSize: number | string;
    notificationMarginBottom: number;
    notificationMarginEdge: number;
    notificationStackLayer: number;
}
export declare const genNoticeStyle: (token: NotificationToken) => CSSObject;
export declare const prepareComponentToken: (token: AliasToken) => {
    zIndexPopup: number;
    width: number;
    closeBtnHoverBg: string;
};
export declare const prepareNotificationToken: (token: Parameters<GenStyleFn<'Notification'>>[0]) => NotificationToken;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
