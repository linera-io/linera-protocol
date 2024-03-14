/// <reference types="react" />
import type { CSSObject } from '@ant-design/cssinjs';
import type { GetDefaultToken } from '../../theme/internal';
export type ComponentToken = {
    /**
     * @desc 星星颜色
     * @descEN Star color
     */
    starColor: string;
    /**
     * @desc 星星大小
     * @descEN Star size
     */
    starSize: number;
    /**
     * @desc 星星悬浮时的缩放
     * @descEN Scale of star when hover
     */
    starHoverScale: CSSObject['transform'];
    /**
     * @desc 星星背景色
     * @descEN Star background color
     */
    starBg: string;
};
export declare const prepareComponentToken: GetDefaultToken<'Rate'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
