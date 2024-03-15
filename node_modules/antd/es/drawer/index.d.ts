import * as React from 'react';
import type { DrawerProps as RcDrawerProps } from 'rc-drawer';
import type { Placement } from 'rc-drawer/lib/Drawer';
import type { DrawerClassNames, DrawerPanelProps, DrawerStyles } from './DrawerPanel';
declare const SizeTypes: readonly ["default", "large"];
type sizeType = (typeof SizeTypes)[number];
export interface PushState {
    distance: string | number;
}
export interface DrawerProps extends Omit<RcDrawerProps, 'maskStyle'>, Omit<DrawerPanelProps, 'prefixCls'> {
    size?: sizeType;
    open?: boolean;
    afterOpenChange?: (open: boolean) => void;
    /** @deprecated Please use `open` instead */
    visible?: boolean;
    /** @deprecated Please use `afterOpenChange` instead */
    afterVisibleChange?: (open: boolean) => void;
    classNames?: DrawerClassNames;
    styles?: DrawerStyles;
}
declare const Drawer: React.FC<DrawerProps> & {
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
};
interface PurePanelInterface {
    prefixCls?: string;
    style?: React.CSSProperties;
    className?: string;
    placement?: Placement;
}
/** @private Internal Component. Do not use in your production. */
declare const PurePanel: React.FC<Omit<DrawerPanelProps, 'prefixCls'> & PurePanelInterface>;
export default Drawer;
