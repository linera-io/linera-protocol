/// <reference types="react" />
import type { FullToken } from '../../theme/internal';
export interface ComponentToken {
}
export interface ColorPickerToken extends FullToken<'ColorPicker'> {
    colorPickerWidth: number;
    colorPickerInsetShadow: string;
    colorPickerHandlerSize: number;
    colorPickerHandlerSizeSM: number;
    colorPickerSliderHeight: number;
    colorPickerPreviewSize: number;
    colorPickerAlphaInputWidth: number;
    colorPickerInputNumberHandleWidth: number;
    colorPickerPresetColorSize: number;
}
export declare const genActiveStyle: (token: ColorPickerToken, borderColor: string, outlineColor: string) => {
    borderInlineEndWidth: number;
    borderColor: string;
    boxShadow: string;
    outline: number;
};
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
