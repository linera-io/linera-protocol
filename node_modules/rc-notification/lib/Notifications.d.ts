import * as React from 'react';
import type { ReactElement } from 'react';
import type { CSSMotionProps } from 'rc-motion';
import type { OpenConfig, Placement, StackConfig } from './interface';
export interface NotificationsProps {
    prefixCls?: string;
    motion?: CSSMotionProps | ((placement: Placement) => CSSMotionProps);
    container?: HTMLElement | ShadowRoot;
    maxCount?: number;
    className?: (placement: Placement) => string;
    style?: (placement: Placement) => React.CSSProperties;
    onAllRemoved?: VoidFunction;
    stack?: StackConfig;
    renderNotifications?: (node: ReactElement, info: {
        prefixCls: string;
        key: React.Key;
    }) => ReactElement;
}
export interface NotificationsRef {
    open: (config: OpenConfig) => void;
    close: (key: React.Key) => void;
    destroy: () => void;
}
declare const Notifications: React.ForwardRefExoticComponent<NotificationsProps & React.RefAttributes<NotificationsRef>>;
export default Notifications;
