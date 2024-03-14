import * as React from 'react';
import type { EditableConfig, Tab } from '../interface';
export interface TabNodeProps {
    id: string;
    prefixCls: string;
    tab: Tab;
    active: boolean;
    closable?: boolean;
    editable?: EditableConfig;
    onClick?: (e: React.MouseEvent | React.KeyboardEvent) => void;
    onResize?: (width: number, height: number, left: number, top: number) => void;
    renderWrapper?: (node: React.ReactElement) => React.ReactElement;
    removeAriaLabel?: string;
    removeIcon?: React.ReactNode;
    onFocus: React.FocusEventHandler;
    style?: React.CSSProperties;
}
declare const TabNode: React.FC<TabNodeProps>;
export default TabNode;
