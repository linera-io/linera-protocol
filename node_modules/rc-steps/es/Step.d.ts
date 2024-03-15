import * as React from 'react';
import type { Status, Icons } from './interface';
import type { StepIconRender, ProgressDotRender } from './Steps';
export interface StepProps {
    prefixCls?: string;
    className?: string;
    style?: React.CSSProperties;
    wrapperStyle?: React.CSSProperties;
    iconPrefix?: string;
    active?: boolean;
    disabled?: boolean;
    stepIndex?: number;
    stepNumber?: number;
    status?: Status;
    title?: React.ReactNode;
    subTitle?: React.ReactNode;
    description?: React.ReactNode;
    tailContent?: React.ReactNode;
    icon?: React.ReactNode;
    icons?: Icons;
    onClick?: React.MouseEventHandler<HTMLDivElement>;
    onStepClick?: (index: number) => void;
    progressDot?: ProgressDotRender | boolean;
    stepIcon?: StepIconRender;
    render?: (stepItem: React.ReactElement) => React.ReactNode;
}
declare function Step(props: StepProps): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
export default Step;
