import * as React from 'react';
import type { NoticeConfig } from './interface';
export interface NoticeProps extends Omit<NoticeConfig, 'onClose'> {
    prefixCls: string;
    className?: string;
    style?: React.CSSProperties;
    eventKey: React.Key;
    onClick?: React.MouseEventHandler<HTMLDivElement>;
    onNoticeClose?: (key: React.Key) => void;
    hovering?: boolean;
}
declare const Notify: React.ForwardRefExoticComponent<NoticeProps & {
    times?: number;
} & React.RefAttributes<HTMLDivElement>>;
export default Notify;
