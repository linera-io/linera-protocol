import * as React from 'react';
export interface SiderContextProps {
    siderCollapsed?: boolean;
}
export declare const SiderContext: React.Context<SiderContextProps>;
export type CollapseType = 'clickTrigger' | 'responsive';
export type SiderTheme = 'light' | 'dark';
export interface SiderProps extends React.HTMLAttributes<HTMLDivElement> {
    prefixCls?: string;
    collapsible?: boolean;
    collapsed?: boolean;
    defaultCollapsed?: boolean;
    reverseArrow?: boolean;
    onCollapse?: (collapsed: boolean, type: CollapseType) => void;
    zeroWidthTriggerStyle?: React.CSSProperties;
    trigger?: React.ReactNode;
    width?: number | string;
    collapsedWidth?: number | string;
    breakpoint?: 'xs' | 'sm' | 'md' | 'lg' | 'xl' | 'xxl';
    theme?: SiderTheme;
    onBreakpoint?: (broken: boolean) => void;
}
export interface SiderState {
    collapsed?: boolean;
    below: boolean;
}
declare const Sider: React.ForwardRefExoticComponent<SiderProps & React.RefAttributes<HTMLDivElement>>;
export default Sider;
