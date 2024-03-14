import type { CSSMotionProps } from 'rc-motion';
import type * as React from 'react';
export type CollapsibleType = 'header' | 'icon' | 'disabled';
export interface ItemType extends Omit<CollapsePanelProps, 'header' | 'prefixCls' | 'panelKey' | 'isActive' | 'accordion' | 'openMotion' | 'expandIcon'> {
    key?: CollapsePanelProps['panelKey'];
    label?: CollapsePanelProps['header'];
    ref?: React.RefObject<HTMLDivElement>;
}
export interface CollapseProps {
    prefixCls?: string;
    activeKey?: React.Key | React.Key[];
    defaultActiveKey?: React.Key | React.Key[];
    openMotion?: CSSMotionProps;
    onChange?: (key: React.Key | React.Key[]) => void;
    accordion?: boolean;
    className?: string;
    style?: object;
    destroyInactivePanel?: boolean;
    expandIcon?: (props: object) => React.ReactNode;
    collapsible?: CollapsibleType;
    children?: React.ReactNode;
    /**
     * Collapse items content
     * @since 3.6.0
     */
    items?: ItemType[];
}
export interface CollapsePanelProps extends React.DOMAttributes<HTMLDivElement> {
    id?: string;
    header?: string | React.ReactNode;
    prefixCls?: string;
    headerClass?: string;
    showArrow?: boolean;
    className?: string;
    style?: object;
    isActive?: boolean;
    openMotion?: CSSMotionProps;
    destroyInactivePanel?: boolean;
    accordion?: boolean;
    forceRender?: boolean;
    extra?: string | React.ReactNode;
    onItemClick?: (panelKey: React.Key) => void;
    expandIcon?: (props: object) => React.ReactNode;
    panelKey?: React.Key;
    role?: string;
    collapsible?: CollapsibleType;
    children?: React.ReactNode;
}
