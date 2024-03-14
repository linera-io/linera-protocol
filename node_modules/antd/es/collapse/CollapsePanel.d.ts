import * as React from 'react';
export type CollapsibleType = 'header' | 'icon' | 'disabled';
export interface CollapsePanelProps {
    key: string | number;
    header: React.ReactNode;
    /** @deprecated Use `collapsible="disabled"` instead */
    disabled?: boolean;
    className?: string;
    style?: React.CSSProperties;
    showArrow?: boolean;
    prefixCls?: string;
    forceRender?: boolean;
    id?: string;
    extra?: React.ReactNode;
    collapsible?: CollapsibleType;
    children?: React.ReactNode;
}
declare const CollapsePanel: React.ForwardRefExoticComponent<CollapsePanelProps & React.RefAttributes<HTMLDivElement>>;
export default CollapsePanel;
