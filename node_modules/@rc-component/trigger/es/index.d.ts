import type { CSSMotionProps } from 'rc-motion';
import * as React from 'react';
import type { ActionType, AlignType, AnimationType, ArrowTypeOuter, BuildInPlacements, TransitionNameType } from './interface';
export type { ActionType, AlignType, ArrowTypeOuter as ArrowType, BuildInPlacements, };
export interface TriggerRef {
    nativeElement: HTMLElement;
    forceAlign: VoidFunction;
}
export interface TriggerProps {
    children: React.ReactElement;
    action?: ActionType | ActionType[];
    showAction?: ActionType[];
    hideAction?: ActionType[];
    prefixCls?: string;
    zIndex?: number;
    onPopupAlign?: (element: HTMLElement, align: AlignType) => void;
    stretch?: string;
    popupVisible?: boolean;
    defaultPopupVisible?: boolean;
    onPopupVisibleChange?: (visible: boolean) => void;
    afterPopupVisibleChange?: (visible: boolean) => void;
    getPopupContainer?: (node: HTMLElement) => HTMLElement;
    forceRender?: boolean;
    autoDestroy?: boolean;
    /** @deprecated Please use `autoDestroy` instead */
    destroyPopupOnHide?: boolean;
    mask?: boolean;
    maskClosable?: boolean;
    /** Set popup motion. You can ref `rc-motion` for more info. */
    popupMotion?: CSSMotionProps;
    /** Set mask motion. You can ref `rc-motion` for more info. */
    maskMotion?: CSSMotionProps;
    /** @deprecated Please us `popupMotion` instead. */
    popupTransitionName?: TransitionNameType;
    /** @deprecated Please us `popupMotion` instead. */
    popupAnimation?: AnimationType;
    /** @deprecated Please us `maskMotion` instead. */
    maskTransitionName?: TransitionNameType;
    /** @deprecated Please us `maskMotion` instead. */
    maskAnimation?: AnimationType;
    mouseEnterDelay?: number;
    mouseLeaveDelay?: number;
    focusDelay?: number;
    blurDelay?: number;
    popup: React.ReactNode | (() => React.ReactNode);
    popupPlacement?: string;
    builtinPlacements?: BuildInPlacements;
    popupAlign?: AlignType;
    popupClassName?: string;
    popupStyle?: React.CSSProperties;
    getPopupClassNameFromAlign?: (align: AlignType) => string;
    onPopupClick?: React.MouseEventHandler<HTMLDivElement>;
    alignPoint?: boolean;
    /**
     * Trigger will memo content when close.
     * This may affect the case if want to keep content update.
     * Set `fresh` to `false` will always keep update.
     */
    fresh?: boolean;
    arrow?: boolean | ArrowTypeOuter;
    /** @deprecated Add `className` on `children`. Please add `className` directly instead. */
    className?: string;
    /**
     * @private Get trigger DOM node.
     * Used for some component is function component which can not access by `findDOMNode`
     */
    getTriggerDOMNode?: (node: React.ReactInstance) => HTMLElement;
}
export declare function generateTrigger(PortalComponent?: React.ComponentType<any>): React.ForwardRefExoticComponent<TriggerProps & React.RefAttributes<TriggerRef>>;
declare const _default: React.ForwardRefExoticComponent<TriggerProps & React.RefAttributes<TriggerRef>>;
export default _default;
