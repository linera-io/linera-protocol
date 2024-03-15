import * as React from 'react';
import type { AlignType } from '@rc-component/trigger';
import type { AdjustOverflow } from '../_util/placements';
import type { MenuProps } from '../menu';
declare const Placements: readonly ["topLeft", "topCenter", "topRight", "bottomLeft", "bottomCenter", "bottomRight", "top", "bottom"];
type Placement = (typeof Placements)[number];
type OverlayFunc = () => React.ReactElement;
export type DropdownArrowOptions = {
    pointAtCenter?: boolean;
};
export interface DropdownProps {
    menu?: MenuProps;
    autoFocus?: boolean;
    arrow?: boolean | DropdownArrowOptions;
    trigger?: ('click' | 'hover' | 'contextMenu')[];
    dropdownRender?: (originNode: React.ReactNode) => React.ReactNode;
    onOpenChange?: (open: boolean, info: {
        source: 'trigger' | 'menu';
    }) => void;
    open?: boolean;
    disabled?: boolean;
    destroyPopupOnHide?: boolean;
    align?: AlignType;
    getPopupContainer?: (triggerNode: HTMLElement) => HTMLElement;
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    transitionName?: string;
    placement?: Placement;
    overlayClassName?: string;
    overlayStyle?: React.CSSProperties;
    forceRender?: boolean;
    mouseEnterDelay?: number;
    mouseLeaveDelay?: number;
    openClassName?: string;
    children?: React.ReactNode;
    autoAdjustOverflow?: boolean | AdjustOverflow;
    /** @deprecated Please use `menu` instead */
    overlay?: React.ReactElement | OverlayFunc;
    /** @deprecated Please use `open` instead */
    visible?: boolean;
    /** @deprecated Please use `onOpenChange` instead */
    onVisibleChange?: (open: boolean) => void;
}
type CompoundedComponent = React.FC<DropdownProps> & {
    _InternalPanelDoNotUseOrYouWillBeFired: typeof WrapPurePanel;
};
declare const Dropdown: CompoundedComponent;
declare const WrapPurePanel: React.FC<DropdownProps>;
export default Dropdown;
