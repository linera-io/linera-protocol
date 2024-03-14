import * as React from 'react';
import type { ProgressProps } from '..';
import type { StrokeColorObject } from '../interface';
export interface ColorGradientProps {
    prefixCls: string;
    gradientId: string;
    style: React.CSSProperties;
    ptg: number;
    radius: number;
    strokeLinecap: ProgressProps['strokeLinecap'];
    strokeWidth: ProgressProps['strokeWidth'];
    size: number;
    color: string | StrokeColorObject;
    gapDegree: number;
}
declare const PtgCircle: React.ForwardRefExoticComponent<ColorGradientProps & React.RefAttributes<SVGCircleElement>>;
export default PtgCircle;
