import type { CircleProps } from './Circle';
import type { ProgressProps } from './progress';
export declare function validProgress(progress?: number): number;
export declare function getSuccessPercent({ success, successPercent }: ProgressProps): number | undefined;
export declare const getPercentage: ({ percent, success, successPercent }: ProgressProps) => number[];
export declare const getStrokeColor: ({ success, strokeColor, }: Partial<CircleProps>) => (string | Record<PropertyKey, string>)[];
export declare const getSize: (size: ProgressProps['size'], type: ProgressProps['type'] | 'step', extra?: {
    steps?: number;
    strokeWidth?: number;
}) => [number, number];
