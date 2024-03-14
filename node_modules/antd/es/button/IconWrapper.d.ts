import React from 'react';
export type IconWrapperProps = {
    prefixCls: string;
    className?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
};
declare const IconWrapper: React.ForwardRefExoticComponent<IconWrapperProps & React.RefAttributes<HTMLSpanElement>>;
export default IconWrapper;
