import type * as React from 'react';
interface ItemSharedProps {
    style?: React.CSSProperties;
    className?: string;
}
export interface SubMenuType extends ItemSharedProps {
    label?: React.ReactNode;
    children: ItemType[];
    disabled?: boolean;
    key: string;
    rootClassName?: string;
    itemIcon?: RenderIconType;
    expandIcon?: RenderIconType;
    onMouseEnter?: MenuHoverEventHandler;
    onMouseLeave?: MenuHoverEventHandler;
    popupClassName?: string;
    popupOffset?: number[];
    popupStyle?: React.CSSProperties;
    onClick?: MenuClickEventHandler;
    onTitleClick?: (info: MenuTitleInfo) => void;
    onTitleMouseEnter?: MenuHoverEventHandler;
    onTitleMouseLeave?: MenuHoverEventHandler;
}
export interface MenuItemType extends ItemSharedProps {
    label?: React.ReactNode;
    disabled?: boolean;
    itemIcon?: RenderIconType;
    key: React.Key;
    onMouseEnter?: MenuHoverEventHandler;
    onMouseLeave?: MenuHoverEventHandler;
    onClick?: MenuClickEventHandler;
}
export interface MenuItemGroupType extends ItemSharedProps {
    type: 'group';
    label?: React.ReactNode;
    children?: ItemType[];
}
export interface MenuDividerType extends ItemSharedProps {
    type: 'divider';
}
export type ItemType = SubMenuType | MenuItemType | MenuItemGroupType | MenuDividerType | null;
export type MenuMode = 'horizontal' | 'vertical' | 'inline';
export type BuiltinPlacements = Record<string, any>;
export type TriggerSubMenuAction = 'click' | 'hover';
export interface RenderIconInfo {
    isSelected?: boolean;
    isOpen?: boolean;
    isSubMenu?: boolean;
    disabled?: boolean;
}
export type RenderIconType = React.ReactNode | ((props: RenderIconInfo) => React.ReactNode);
export interface MenuInfo {
    key: string;
    keyPath: string[];
    /** @deprecated This will not support in future. You should avoid to use this */
    item: React.ReactInstance;
    domEvent: React.MouseEvent<HTMLElement> | React.KeyboardEvent<HTMLElement>;
}
export interface MenuTitleInfo {
    key: string;
    domEvent: React.MouseEvent<HTMLElement> | React.KeyboardEvent<HTMLElement>;
}
export type MenuHoverEventHandler = (info: {
    key: string;
    domEvent: React.MouseEvent<HTMLElement>;
}) => void;
export interface SelectInfo extends MenuInfo {
    selectedKeys: string[];
}
export type SelectEventHandler = (info: SelectInfo) => void;
export type MenuClickEventHandler = (info: MenuInfo) => void;
export type MenuRef = {
    /**
     * Focus active child if any, or the first child which is not disabled will be focused.
     * @param options
     */
    focus: (options?: FocusOptions) => void;
    list: HTMLUListElement;
};
export {};
