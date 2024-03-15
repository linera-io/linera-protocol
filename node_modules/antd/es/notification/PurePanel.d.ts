import * as React from 'react';
import type { NoticeProps } from 'rc-notification/lib/Notice';
import type { IconType } from './interface';
export declare const TypeIcon: {
    info: React.JSX.Element;
    success: React.JSX.Element;
    error: React.JSX.Element;
    warning: React.JSX.Element;
    loading: React.JSX.Element;
};
export declare function getCloseIcon(prefixCls: string, closeIcon?: React.ReactNode): React.ReactNode;
export interface PureContentProps {
    prefixCls: string;
    icon?: React.ReactNode;
    message?: React.ReactNode;
    description?: React.ReactNode;
    btn?: React.ReactNode;
    type?: IconType;
    role?: 'alert' | 'status';
}
export declare const PureContent: React.FC<PureContentProps>;
export interface PurePanelProps extends Omit<NoticeProps, 'prefixCls' | 'eventKey'>, Omit<PureContentProps, 'prefixCls' | 'children'> {
    prefixCls?: string;
}
/** @private Internal Component. Do not use in your production. */
declare const PurePanel: React.FC<PurePanelProps>;
export default PurePanel;
