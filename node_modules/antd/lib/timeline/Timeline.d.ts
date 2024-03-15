import * as React from 'react';
import type { TimelineItemProps } from './TimelineItem';
export interface TimelineProps {
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    /** 指定最后一个幽灵节点是否存在或内容 */
    pending?: React.ReactNode;
    pendingDot?: React.ReactNode;
    style?: React.CSSProperties;
    reverse?: boolean;
    mode?: 'left' | 'alternate' | 'right';
    items?: TimelineItemProps[];
    children?: React.ReactNode;
}
type CompoundedComponent = React.FC<TimelineProps> & {
    Item: React.FC<TimelineItemProps>;
};
declare const Timeline: CompoundedComponent;
export default Timeline;
