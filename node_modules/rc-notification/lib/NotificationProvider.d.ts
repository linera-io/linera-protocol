import type { FC } from 'react';
import React from 'react';
export interface NotificationContextProps {
    classNames?: {
        notice?: string;
        list?: string;
    };
}
export declare const NotificationContext: React.Context<NotificationContextProps>;
export interface NotificationProviderProps extends NotificationContextProps {
    children: React.ReactNode;
}
declare const NotificationProvider: FC<NotificationProviderProps>;
export default NotificationProvider;
