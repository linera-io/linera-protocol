import * as React from 'react';
export interface SkeletonElementProps {
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    size?: 'large' | 'small' | 'default' | number;
    shape?: 'circle' | 'square' | 'round' | 'default';
    active?: boolean;
}
declare const Element: React.FC<SkeletonElementProps>;
export default Element;
