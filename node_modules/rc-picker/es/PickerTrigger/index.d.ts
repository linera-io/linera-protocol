import type { AlignType, BuildInPlacements } from '@rc-component/trigger/lib/interface';
import * as React from 'react';
export type PickerTriggerProps = {
    popupElement: React.ReactElement;
    popupStyle?: React.CSSProperties;
    children: React.ReactElement;
    transitionName?: string;
    getPopupContainer?: (node: HTMLElement) => HTMLElement;
    popupAlign?: AlignType;
    range?: boolean;
    popupClassName?: string;
    placement?: string;
    builtinPlacements?: BuildInPlacements;
    direction?: 'ltr' | 'rtl';
    visible: boolean;
    onClose: () => void;
};
declare function PickerTrigger({ popupElement, popupStyle, popupClassName, popupAlign, transitionName, getPopupContainer, children, range, placement, builtinPlacements, direction, visible, onClose, }: PickerTriggerProps): React.JSX.Element;
export default PickerTrigger;
