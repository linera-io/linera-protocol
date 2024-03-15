import * as React from 'react';
import type { ProgressGradient, ProgressProps } from './progress';
export interface CircleProps extends ProgressProps {
    prefixCls: string;
    children: React.ReactNode;
    progressStatus: string;
    strokeColor?: string | ProgressGradient;
}
declare const Circle: React.FC<CircleProps>;
export default Circle;
