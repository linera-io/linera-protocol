import * as React from 'react';
export interface GroupProps {
    className?: string;
    size?: 'large' | 'small' | 'default';
    children?: React.ReactNode;
    style?: React.CSSProperties;
    onMouseEnter?: React.MouseEventHandler<HTMLSpanElement>;
    onMouseLeave?: React.MouseEventHandler<HTMLSpanElement>;
    onFocus?: React.FocusEventHandler<HTMLSpanElement>;
    onBlur?: React.FocusEventHandler<HTMLSpanElement>;
    prefixCls?: string;
    compact?: boolean;
}
declare const Group: React.FC<GroupProps>;
export default Group;
