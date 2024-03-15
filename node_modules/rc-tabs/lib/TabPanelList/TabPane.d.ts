import * as React from 'react';
export interface TabPaneProps {
    tab?: React.ReactNode;
    className?: string;
    style?: React.CSSProperties;
    disabled?: boolean;
    children?: React.ReactNode;
    forceRender?: boolean;
    closable?: boolean;
    closeIcon?: React.ReactNode;
    icon?: React.ReactNode;
    prefixCls?: string;
    tabKey?: string;
    id?: string;
    animated?: boolean;
    active?: boolean;
    destroyInactiveTabPane?: boolean;
}
declare const TabPane: React.ForwardRefExoticComponent<TabPaneProps & React.RefAttributes<HTMLDivElement>>;
export default TabPane;
