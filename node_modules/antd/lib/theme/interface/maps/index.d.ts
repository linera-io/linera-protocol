import type { ColorPalettes, LegacyColorPalettes } from '../presetColors';
import type { SeedToken } from '../seeds';
import type { ColorMapToken } from './colors';
import type { FontMapToken } from './font';
import type { HeightMapToken, SizeMapToken } from './size';
import type { StyleMapToken } from './style';
export * from './colors';
export * from './font';
export * from './size';
export * from './style';
export interface CommonMapToken extends StyleMapToken {
    /**
     * @desc 动效播放速度，快速。用于小型元素动画交互
     * @descEN Motion speed, fast speed. Used for small element animation interaction.
     */
    motionDurationFast: string;
    /**
     * @desc 动效播放速度，中速。用于中型元素动画交互
     * @descEN Motion speed, medium speed. Used for medium element animation interaction.
     */
    motionDurationMid: string;
    /**
     * @desc 动效播放速度，慢速。用于大型元素如面板动画交互
     * @descEN Motion speed, slow speed. Used for large element animation interaction.
     */
    motionDurationSlow: string;
}
export interface MapToken extends SeedToken, ColorPalettes, LegacyColorPalettes, ColorMapToken, SizeMapToken, HeightMapToken, StyleMapToken, FontMapToken, CommonMapToken {
}
