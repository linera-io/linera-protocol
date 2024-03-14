import type { ColorGenInput } from '@rc-component/color-picker';
import { Color as RcColor } from '@rc-component/color-picker';
export declare const toHexFormat: (value?: string, alpha?: boolean) => string;
export declare const getHex: (value?: string, alpha?: boolean) => string;
export interface Color extends Pick<RcColor, 'toHsb' | 'toHsbString' | 'toHex' | 'toHexString' | 'toRgb' | 'toRgbString'> {
}
export declare class ColorFactory {
    /** Original Color object */
    private metaColor;
    constructor(color: ColorGenInput<Color>);
    toHsb(): import("@ctrl/tinycolor").Numberify<import("@rc-component/color-picker").HSBA>;
    toHsbString(): string;
    toHex(): string;
    toHexString(): string;
    toRgb(): import("@ctrl/tinycolor").Numberify<import("@ctrl/tinycolor").RGBA>;
    toRgbString(): string;
}
