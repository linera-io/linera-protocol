import type { AlignType, BuildInPlacements } from '@rc-component/trigger/lib/interface';
import * as React from 'react';
import type { Placement, RenderDOMFunc } from './BaseSelect';
export interface RefTriggerProps {
    getPopupElement: () => HTMLDivElement;
}
export interface SelectTriggerProps {
    prefixCls: string;
    children: React.ReactElement;
    disabled: boolean;
    visible: boolean;
    popupElement: React.ReactElement;
    animation?: string;
    transitionName?: string;
    placement?: Placement;
    builtinPlacements?: BuildInPlacements;
    dropdownStyle: React.CSSProperties;
    dropdownClassName: string;
    direction: string;
    dropdownMatchSelectWidth?: boolean | number;
    dropdownRender?: (menu: React.ReactElement) => React.ReactElement;
    getPopupContainer?: RenderDOMFunc;
    dropdownAlign: AlignType;
    empty: boolean;
    getTriggerDOMNode: () => HTMLElement;
    onPopupVisibleChange?: (visible: boolean) => void;
    onPopupMouseEnter: () => void;
}
declare const RefSelectTrigger: React.ForwardRefExoticComponent<SelectTriggerProps & React.RefAttributes<RefTriggerProps>>;
export default RefSelectTrigger;
