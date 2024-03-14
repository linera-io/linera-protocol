import * as React from 'react';
import type { AvatarSize } from './AvatarContext';
export interface GroupProps {
    className?: string;
    rootClassName?: string;
    children?: React.ReactNode;
    style?: React.CSSProperties;
    prefixCls?: string;
    maxCount?: number;
    maxStyle?: React.CSSProperties;
    maxPopoverPlacement?: 'top' | 'bottom';
    maxPopoverTrigger?: 'hover' | 'focus' | 'click';
    size?: AvatarSize;
    shape?: 'circle' | 'square';
}
declare const Group: React.FC<GroupProps>;
export default Group;
