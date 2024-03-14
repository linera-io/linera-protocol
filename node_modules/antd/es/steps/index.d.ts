import RcSteps from 'rc-steps';
import type { ProgressDotRender } from 'rc-steps/lib/Steps';
import * as React from 'react';
export interface StepProps {
    className?: string;
    description?: React.ReactNode;
    icon?: React.ReactNode;
    onClick?: React.MouseEventHandler<HTMLElement>;
    status?: 'wait' | 'process' | 'finish' | 'error';
    disabled?: boolean;
    title?: React.ReactNode;
    subTitle?: React.ReactNode;
    style?: React.CSSProperties;
}
export interface StepsProps {
    type?: 'default' | 'navigation' | 'inline';
    className?: string;
    rootClassName?: string;
    current?: number;
    direction?: 'horizontal' | 'vertical';
    iconPrefix?: string;
    initial?: number;
    labelPlacement?: 'horizontal' | 'vertical';
    prefixCls?: string;
    progressDot?: boolean | ProgressDotRender;
    responsive?: boolean;
    size?: 'default' | 'small';
    status?: 'wait' | 'process' | 'finish' | 'error';
    style?: React.CSSProperties;
    percent?: number;
    onChange?: (current: number) => void;
    children?: React.ReactNode;
    items?: StepProps[];
}
type CompoundedComponent = React.FC<StepsProps> & {
    Step: typeof RcSteps.Step;
};
declare const Steps: CompoundedComponent;
export default Steps;
