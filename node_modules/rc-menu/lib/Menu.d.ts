import type { CSSMotionProps } from 'rc-motion';
import * as React from 'react';
import type { BuiltinPlacements, ItemType, MenuClickEventHandler, MenuMode, MenuRef, RenderIconType, SelectEventHandler, TriggerSubMenuAction } from './interface';
export interface MenuProps extends Omit<React.HTMLAttributes<HTMLUListElement>, 'onClick' | 'onSelect' | 'dir'> {
    prefixCls?: string;
    rootClassName?: string;
    items?: ItemType[];
    /** @deprecated Please use `items` instead */
    children?: React.ReactNode;
    disabled?: boolean;
    /** @private Disable auto overflow. Pls note the prop name may refactor since we do not final decided. */
    disabledOverflow?: boolean;
    /** direction of menu */
    direction?: 'ltr' | 'rtl';
    mode?: MenuMode;
    inlineCollapsed?: boolean;
    defaultOpenKeys?: string[];
    openKeys?: string[];
    activeKey?: string;
    defaultActiveFirst?: boolean;
    selectable?: boolean;
    multiple?: boolean;
    defaultSelectedKeys?: string[];
    selectedKeys?: string[];
    onSelect?: SelectEventHandler;
    onDeselect?: SelectEventHandler;
    inlineIndent?: number;
    /** Menu motion define. Use `defaultMotions` if you need config motion of each mode */
    motion?: CSSMotionProps;
    /** Default menu motion of each mode */
    defaultMotions?: Partial<{
        [key in MenuMode | 'other']: CSSMotionProps;
    }>;
    subMenuOpenDelay?: number;
    subMenuCloseDelay?: number;
    forceSubMenuRender?: boolean;
    triggerSubMenuAction?: TriggerSubMenuAction;
    builtinPlacements?: BuiltinPlacements;
    itemIcon?: RenderIconType;
    expandIcon?: RenderIconType;
    overflowedIndicator?: React.ReactNode;
    /** @private Internal usage. Do not use in your production. */
    overflowedIndicatorPopupClassName?: string;
    getPopupContainer?: (node: HTMLElement) => HTMLElement;
    onClick?: MenuClickEventHandler;
    onOpenChange?: (openKeys: string[]) => void;
    /***
     * @private Only used for `pro-layout`. Do not use in your prod directly
     * and we do not promise any compatibility for this.
     */
    _internalRenderMenuItem?: (originNode: React.ReactElement, menuItemProps: any, stateProps: {
        selected: boolean;
    }) => React.ReactElement;
    /***
     * @private Only used for `pro-layout`. Do not use in your prod directly
     * and we do not promise any compatibility for this.
     */
    _internalRenderSubMenuItem?: (originNode: React.ReactElement, subMenuItemProps: any, stateProps: {
        selected: boolean;
        open: boolean;
        active: boolean;
        disabled: boolean;
    }) => React.ReactElement;
}
declare const Menu: React.ForwardRefExoticComponent<MenuProps & React.RefAttributes<MenuRef>>;
export default Menu;
