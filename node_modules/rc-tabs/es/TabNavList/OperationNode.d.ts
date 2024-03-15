import * as React from 'react';
import type { EditableConfig, Tab, TabsLocale } from '../interface';
export interface OperationNodeProps {
    prefixCls: string;
    className?: string;
    style?: React.CSSProperties;
    id: string;
    tabs: Tab[];
    rtl: boolean;
    tabBarGutter?: number;
    activeKey: string;
    mobile: boolean;
    moreIcon?: React.ReactNode;
    moreTransitionName?: string;
    editable?: EditableConfig;
    locale?: TabsLocale;
    removeAriaLabel?: string;
    onTabClick: (key: string, e: React.MouseEvent | React.KeyboardEvent) => void;
    tabMoving?: boolean;
    getPopupContainer?: (node: HTMLElement) => HTMLElement;
    popupClassName?: string;
}
declare const _default: React.MemoExoticComponent<React.ForwardRefExoticComponent<OperationNodeProps & React.RefAttributes<HTMLDivElement>>>;
export default _default;
