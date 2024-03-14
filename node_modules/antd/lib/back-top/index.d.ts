import * as React from 'react';
export interface BackTopProps {
    visibilityHeight?: number;
    onClick?: React.MouseEventHandler<HTMLElement>;
    target?: () => HTMLElement | Window | Document;
    prefixCls?: string;
    children?: React.ReactNode;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    duration?: number;
}
declare const BackTop: React.FC<BackTopProps>;
export default BackTop;
