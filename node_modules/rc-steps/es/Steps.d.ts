import React from 'react';
import type { Icons, Status } from './interface';
import type { StepProps } from './Step';
export declare type StepIconRender = (info: {
    index: number;
    status: Status;
    title: React.ReactNode;
    description: React.ReactNode;
    node: React.ReactNode;
}) => React.ReactNode;
export declare type ProgressDotRender = (iconDot: any, info: {
    index: number;
    status: Status;
    title: React.ReactNode;
    description: React.ReactNode;
}) => React.ReactNode;
export interface StepsProps {
    prefixCls?: string;
    style?: React.CSSProperties;
    className?: string;
    children?: React.ReactNode;
    direction?: 'horizontal' | 'vertical';
    type?: 'default' | 'navigation' | 'inline';
    labelPlacement?: 'horizontal' | 'vertical';
    iconPrefix?: string;
    status?: Status;
    size?: 'default' | 'small';
    current?: number;
    progressDot?: ProgressDotRender | boolean;
    stepIcon?: StepIconRender;
    initial?: number;
    icons?: Icons;
    items?: StepProps[];
    itemRender?: (item: StepProps, stepItem: React.ReactElement) => React.ReactNode;
    onChange?: (current: number) => void;
}
declare function Steps(props: StepsProps): React.JSX.Element;
declare namespace Steps {
    var Step: typeof import("./Step").default;
}
export default Steps;
