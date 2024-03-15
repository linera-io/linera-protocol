import type { StrokeColorType } from '../interface';
import type { ProgressProps } from '..';
export declare const VIEW_BOX_SIZE = 100;
export declare const getCircleStyle: (perimeter: number, perimeterWithoutGap: number, offset: number, percent: number, rotateDeg: number, gapDegree: any, gapPosition: ProgressProps['gapPosition'] | undefined, strokeColor: StrokeColorType, strokeLinecap: ProgressProps['strokeLinecap'], strokeWidth: any, stepSpace?: number) => {
    stroke: string;
    strokeDasharray: string;
    strokeDashoffset: number;
    transform: string;
    transformOrigin: string;
    transition: string;
    fillOpacity: number;
};
