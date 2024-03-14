import * as React from 'react';
export type ScrollBarDirectionType = 'ltr' | 'rtl';
export interface ScrollBarProps {
    prefixCls: string;
    scrollOffset: number;
    scrollRange: number;
    rtl: boolean;
    onScroll: (scrollOffset: number, horizontal?: boolean) => void;
    onStartMove: () => void;
    onStopMove: () => void;
    horizontal?: boolean;
    style?: React.CSSProperties;
    thumbStyle?: React.CSSProperties;
    spinSize: number;
    containerSize: number;
}
export interface ScrollBarRef {
    delayHidden: () => void;
}
declare const ScrollBar: React.ForwardRefExoticComponent<ScrollBarProps & React.RefAttributes<ScrollBarRef>>;
export default ScrollBar;
