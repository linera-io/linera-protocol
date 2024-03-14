import type { CSSProperties } from 'react';
import type { FullToken, GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /** @deprecated Use headerBg instead */
    colorBgHeader: string;
    /** @deprecated Use bodyBg instead */
    colorBgBody: string;
    /** @deprecated Use triggerBg instead */
    colorBgTrigger: string;
    /**
     * @desc 主体部分背景色
     * @descEN Background Color of body
     */
    bodyBg: string;
    /**
     * @desc 顶部背景色
     * @descEN Background Color of header
     */
    headerBg: string;
    /**
     * @desc 顶部高度
     * @descEN Height of header
     */
    headerHeight: number;
    /**
     * @desc 顶部内边距
     * @descEN Padding of header
     */
    headerPadding: CSSProperties['padding'];
    /**
     * @desc 顶部文字颜色
     * @descEN Text color of header
     */
    headerColor: string;
    /**
     * @desc 页脚内边距
     * @descEN Padding of footer
     */
    footerPadding: CSSProperties['padding'];
    /**
     * @desc 页脚背景色
     * @descEN Background Color of footer
     */
    footerBg: string;
    /**
     * @desc 侧边栏背景色
     * @descEN Background Color of sider
     */
    siderBg: string;
    /**
     * @desc 侧边栏开关高度
     * @descEN Height of sider trigger
     */
    triggerHeight: number;
    /**
     * @desc 侧边栏开关背景色
     * @descEN Background Color of sider trigger
     */
    triggerBg: string;
    /**
     * @desc 侧边栏开关颜色
     * @descEN Color of sider trigger
     */
    triggerColor: string;
    /**
     * @desc collapse 为 0 时侧边栏开关宽度
     * @descEN Width of sider trigger when collapse is 0
     */
    zeroTriggerWidth: number;
    /**
     * @desc collapse 为 0 时侧边栏开关高度
     * @descEN Height of sider trigger when collapse is 0
     */
    zeroTriggerHeight: number;
    /**
     * @desc 亮色主题侧边栏背景色
     * @descEN Background Color of light theme sider
     */
    lightSiderBg: string;
    /**
     * @desc 亮色主题侧边栏开关背景色
     * @descEN Background Color of light theme sider trigger
     */
    lightTriggerBg: string;
    /**
     * @desc 亮色主题侧边栏开关颜色
     * @descEN Color of light theme sider trigger
     */
    lightTriggerColor: string;
}
export interface LayoutToken extends FullToken<'Layout'> {
}
export declare const prepareComponentToken: GetDefaultToken<'Layout'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
