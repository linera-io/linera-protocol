import type { CSSProperties, FC } from 'react';
import React from 'react';
import type { CSSMotionProps } from 'rc-motion';
import type { OpenConfig, Placement, StackConfig } from './interface';
export interface NoticeListProps {
    configList?: OpenConfig[];
    placement?: Placement;
    prefixCls?: string;
    motion?: CSSMotionProps | ((placement: Placement) => CSSMotionProps);
    stack?: StackConfig;
    onAllNoticeRemoved?: (placement: Placement) => void;
    onNoticeClose?: (key: React.Key) => void;
    className?: string;
    style?: CSSProperties;
}
declare const NoticeList: FC<NoticeListProps>;
export default NoticeList;
