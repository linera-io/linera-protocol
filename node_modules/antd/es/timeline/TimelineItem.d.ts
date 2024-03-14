import * as React from 'react';
import type { LiteralUnion } from '../_util/type';
type Color = 'blue' | 'red' | 'green' | 'gray';
export interface TimelineItemProps {
    key?: React.Key;
    prefixCls?: string;
    className?: string;
    color?: LiteralUnion<Color>;
    dot?: React.ReactNode;
    pending?: boolean;
    position?: string;
    style?: React.CSSProperties;
    label?: React.ReactNode;
    children?: React.ReactNode;
}
export interface TimeLineItemProps extends TimelineItemProps {
    __deprecated_do_not_use_it__?: any;
}
declare const TimelineItem: React.FC<TimelineItemProps>;
export default TimelineItem;
