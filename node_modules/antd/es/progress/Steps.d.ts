import * as React from 'react';
import type { ProgressProps } from './progress';
interface ProgressStepsProps extends ProgressProps {
    steps: number;
    strokeColor?: string | string[];
    trailColor?: string;
}
declare const Steps: React.FC<ProgressStepsProps>;
export default Steps;
