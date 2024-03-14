import * as React from 'react';
import type { DirectionType } from '../config-provider';
import type { ProgressGradient, ProgressProps, StringGradients } from './progress';
interface LineProps extends ProgressProps {
    prefixCls: string;
    direction?: DirectionType;
    children: React.ReactNode;
    strokeColor?: string | ProgressGradient;
}
/**
 * @example
 *   {
 *     "0%": "#afc163",
 *     "75%": "#009900",
 *     "50%": "green", // ====> '#afc163 0%, #66FF00 25%, #00CC00 50%, #009900 75%, #ffffff 100%'
 *     "25%": "#66FF00",
 *     "100%": "#ffffff"
 *   }
 */
export declare const sortGradient: (gradients: StringGradients) => string;
/**
 * Then this man came to realize the truth: Besides six pence, there is the moon. Besides bread and
 * butter, there is the bug. And... Besides women, there is the code.
 *
 * @example
 *   {
 *     "0%": "#afc163",
 *     "25%": "#66FF00",
 *     "50%": "#00CC00", // ====>  linear-gradient(to right, #afc163 0%, #66FF00 25%,
 *     "75%": "#009900", //        #00CC00 50%, #009900 75%, #ffffff 100%)
 *     "100%": "#ffffff"
 *   }
 */
export declare const handleGradient: (strokeColor: ProgressGradient, directionConfig?: DirectionType) => React.CSSProperties;
declare const Line: React.FC<LineProps>;
export default Line;
