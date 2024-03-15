import type { Color } from './color';
export interface HSB {
    h: number | string;
    s: number | string;
    b: number | string;
}
export interface RGB {
    r: number | string;
    g: number | string;
    b: number | string;
}
export interface HSBA extends HSB {
    a: number;
}
export interface RGBA extends RGB {
    a: number;
}
export type ColorGenInput<T = Color> = string | number | RGB | RGBA | HSB | HSBA | T;
export type HsbaColorType = 'hue' | 'alpha';
export type TransformOffset = {
    x: number;
    y: number;
};
export interface BaseColorPickerProps {
    color?: Color;
    prefixCls?: string;
    disabled?: boolean;
    onChange?: (color: Color, type?: HsbaColorType) => void;
    onChangeComplete?: (value: Color, type?: HsbaColorType) => void;
}
