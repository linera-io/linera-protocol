import type { Numberify } from '@ctrl/tinycolor';
import { TinyColor } from '@ctrl/tinycolor';
import type { ColorGenInput, HSBA } from './interface';
export declare const getRoundNumber: (value: number) => number;
export declare class Color extends TinyColor {
    constructor(color: ColorGenInput);
    toHsbString(): string;
    toHsb(): Numberify<HSBA>;
}
