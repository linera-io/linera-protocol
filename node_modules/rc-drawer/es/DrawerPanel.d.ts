import * as React from 'react';
export interface DrawerPanelRef {
    focus: VoidFunction;
}
export interface DrawerPanelEvents {
    onMouseEnter?: React.MouseEventHandler<HTMLDivElement>;
    onMouseOver?: React.MouseEventHandler<HTMLDivElement>;
    onMouseLeave?: React.MouseEventHandler<HTMLDivElement>;
    onClick?: React.MouseEventHandler<HTMLDivElement>;
    onKeyDown?: React.KeyboardEventHandler<HTMLDivElement>;
    onKeyUp?: React.KeyboardEventHandler<HTMLDivElement>;
}
export type DrawerPanelAccessibility = Pick<React.DialogHTMLAttributes<HTMLDivElement>, keyof React.AriaAttributes>;
export interface DrawerPanelProps extends DrawerPanelEvents, DrawerPanelAccessibility {
    prefixCls: string;
    className?: string;
    id?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
    containerRef?: React.Ref<HTMLDivElement>;
}
declare const DrawerPanel: {
    (props: DrawerPanelProps): React.JSX.Element;
    displayName: string;
};
export default DrawerPanel;
