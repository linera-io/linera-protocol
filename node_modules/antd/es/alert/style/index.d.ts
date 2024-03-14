import type { CSSProperties } from 'react';
import type { FullToken, GenerateStyle, GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 默认内间距
     * @descEN Default padding
     */
    defaultPadding: CSSProperties['padding'];
    /**
     * @desc 带有描述的内间距
     * @descEN Padding with description
     */
    withDescriptionPadding: CSSProperties['padding'];
    /**
     * @desc 带有描述时的图标尺寸
     * @descEN Icon size with description
     */
    withDescriptionIconSize: number;
}
type AlertToken = FullToken<'Alert'> & {};
export declare const genBaseStyle: GenerateStyle<AlertToken>;
export declare const genTypeStyle: GenerateStyle<AlertToken>;
export declare const genActionStyle: GenerateStyle<AlertToken>;
export declare const prepareComponentToken: GetDefaultToken<'Alert'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
