import * as React from 'react';
export interface ScrollNumberProps {
    prefixCls?: string;
    className?: string;
    motionClassName?: string;
    count?: string | number | null;
    children?: React.ReactElement<HTMLElement>;
    component?: React.ComponentType<any>;
    style?: React.CSSProperties;
    title?: string | number | null;
    show: boolean;
}
export interface ScrollNumberState {
    animateStarted?: boolean;
    count?: string | number | null;
}
declare const ScrollNumber: React.ForwardRefExoticComponent<ScrollNumberProps & React.RefAttributes<HTMLElement>>;
export default ScrollNumber;
