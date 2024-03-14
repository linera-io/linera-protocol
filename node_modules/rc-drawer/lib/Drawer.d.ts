import type { PortalProps } from '@rc-component/portal';
import * as React from 'react';
import type { DrawerPanelAccessibility, DrawerPanelEvents } from './DrawerPanel';
import type { DrawerPopupProps } from './DrawerPopup';
import type { DrawerClassNames, DrawerStyles } from './inter';
export type Placement = 'left' | 'top' | 'right' | 'bottom';
export interface DrawerProps extends Omit<DrawerPopupProps, 'prefixCls' | 'inline' | 'scrollLocker'>, DrawerPanelEvents, DrawerPanelAccessibility {
    prefixCls?: string;
    open?: boolean;
    onClose?: (e: React.MouseEvent | React.KeyboardEvent) => void;
    destroyOnClose?: boolean;
    getContainer?: PortalProps['getContainer'];
    panelRef?: React.Ref<HTMLDivElement>;
    classNames?: DrawerClassNames;
    styles?: DrawerStyles;
}
declare const Drawer: React.FC<DrawerProps>;
export default Drawer;
