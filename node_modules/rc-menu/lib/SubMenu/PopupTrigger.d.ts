import * as React from 'react';
import type { MenuMode } from '../interface';
export interface PopupTriggerProps {
    prefixCls: string;
    mode: MenuMode;
    visible: boolean;
    children: React.ReactElement;
    popup: React.ReactNode;
    popupStyle?: React.CSSProperties;
    popupClassName?: string;
    popupOffset?: number[];
    disabled: boolean;
    onVisibleChange: (visible: boolean) => void;
}
export default function PopupTrigger({ prefixCls, visible, children, popup, popupStyle, popupClassName, popupOffset, disabled, mode, onVisibleChange, }: PopupTriggerProps): React.JSX.Element;
