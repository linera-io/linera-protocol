import type { TriggerProps } from '@rc-component/trigger';
import type { ActionType, AlignType, AnimationType, BuildInPlacements } from '@rc-component/trigger/lib/interface';
import React from 'react';
import Placements from './placements';
export interface DropdownProps extends Pick<TriggerProps, 'getPopupContainer' | 'children' | 'mouseEnterDelay' | 'mouseLeaveDelay' | 'onPopupAlign' | 'builtinPlacements' | 'autoDestroy'> {
    minOverlayWidthMatchTrigger?: boolean;
    arrow?: boolean;
    onVisibleChange?: (visible: boolean) => void;
    onOverlayClick?: (e: Event) => void;
    prefixCls?: string;
    transitionName?: string;
    overlayClassName?: string;
    openClassName?: string;
    animation?: AnimationType;
    align?: AlignType;
    overlayStyle?: React.CSSProperties;
    placement?: keyof typeof Placements;
    placements?: BuildInPlacements;
    overlay?: (() => React.ReactElement) | React.ReactElement;
    trigger?: ActionType | ActionType[];
    alignPoint?: boolean;
    showAction?: ActionType[];
    hideAction?: ActionType[];
    visible?: boolean;
    autoFocus?: boolean;
}
declare const _default: React.ForwardRefExoticComponent<DropdownProps & React.RefAttributes<unknown>>;
export default _default;
