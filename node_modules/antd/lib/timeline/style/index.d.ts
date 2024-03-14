/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 轨迹颜色
     * @descEN Line color
     */
    tailColor: string;
    /**
     * @desc 轨迹宽度
     * @descEN Line width
     */
    tailWidth: number;
    /**
     * @desc 节点边框宽度
     * @descEN Border width of node
     */
    dotBorderWidth: number;
    /**
     * @desc 节点背景色
     * @descEN Background color of node
     */
    dotBg: string;
    /**
     * @desc 时间项下间距
     * @descEN Bottom padding of item
     */
    itemPaddingBottom: number;
}
export declare const prepareComponentToken: GetDefaultToken<'Timeline'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
