import type { ArrowType, TriggerProps } from '@rc-component/trigger';
import type { ActionType, AlignType, AnimationType } from '@rc-component/trigger/lib/interface';
import * as React from 'react';
export interface TooltipProps extends Pick<TriggerProps, 'onPopupAlign' | 'builtinPlacements' | 'fresh' | 'children' | 'mouseLeaveDelay' | 'mouseEnterDelay' | 'prefixCls' | 'forceRender'> {
    trigger?: ActionType | ActionType[];
    defaultVisible?: boolean;
    visible?: boolean;
    placement?: string;
    /** @deprecated Use `motion` instead */
    transitionName?: string;
    /** @deprecated Use `motion` instead */
    animation?: AnimationType;
    /** Config popup motion */
    motion?: TriggerProps['popupMotion'];
    onVisibleChange?: (visible: boolean) => void;
    afterVisibleChange?: (visible: boolean) => void;
    overlay: (() => React.ReactNode) | React.ReactNode;
    overlayStyle?: React.CSSProperties;
    overlayClassName?: string;
    getTooltipContainer?: (node: HTMLElement) => HTMLElement;
    destroyTooltipOnHide?: boolean;
    align?: AlignType;
    showArrow?: boolean | ArrowType;
    arrowContent?: React.ReactNode;
    id?: string;
    overlayInnerStyle?: React.CSSProperties;
    zIndex?: number;
}
export interface TooltipRef {
    nativeElement: HTMLElement;
    forceAlign: VoidFunction;
}
declare const _default: React.ForwardRefExoticComponent<TooltipProps & React.RefAttributes<TooltipRef>>;
export default _default;
