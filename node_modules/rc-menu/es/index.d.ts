import Menu from './Menu';
import MenuItem from './MenuItem';
import SubMenu from './SubMenu';
import MenuItemGroup from './MenuItemGroup';
import { useFullPath } from './context/PathContext';
import Divider from './Divider';
import type { MenuProps } from './Menu';
import type { MenuItemProps } from './MenuItem';
import type { SubMenuProps } from './SubMenu';
import type { MenuItemGroupProps } from './MenuItemGroup';
import type { MenuRef } from './interface';
export { SubMenu, MenuItem as Item, MenuItem, MenuItemGroup, MenuItemGroup as ItemGroup, Divider, 
/** @private Only used for antd internal. Do not use in your production. */
useFullPath, };
export type { MenuProps, SubMenuProps, MenuItemProps, MenuItemGroupProps, MenuRef, };
type MenuType = typeof Menu & {
    Item: typeof MenuItem;
    SubMenu: typeof SubMenu;
    ItemGroup: typeof MenuItemGroup;
    Divider: typeof Divider;
};
declare const ExportMenu: MenuType;
export default ExportMenu;
