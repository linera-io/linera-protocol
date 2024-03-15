/// <reference types="react" />
export interface ProgressProps {
    id?: string;
    strokeWidth?: number;
    trailWidth?: number;
    className?: string;
    percent?: number | number[];
    strokeColor?: StrokeColorType;
    trailColor?: string;
    strokeLinecap?: StrokeLinecapType;
    prefixCls?: string;
    style?: React.CSSProperties;
    gapDegree?: number;
    gapPosition?: GapPositionType;
    transition?: string;
    onClick?: React.MouseEventHandler;
    steps?: number | {
        count: number;
        space: number;
    };
}
export type StrokeColorObject = Record<string, string>;
export type BaseStrokeColorType = string | StrokeColorObject;
export type StrokeColorType = BaseStrokeColorType | BaseStrokeColorType[];
export type GapPositionType = 'top' | 'right' | 'bottom' | 'left';
export type StrokeLinecapType = 'round' | 'butt' | 'square';
