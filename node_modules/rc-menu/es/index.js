import Menu from "./Menu";
import MenuItem from "./MenuItem";
import SubMenu from "./SubMenu";
import MenuItemGroup from "./MenuItemGroup";
import { useFullPath } from "./context/PathContext";
import Divider from "./Divider";
export { SubMenu, MenuItem as Item, MenuItem, MenuItemGroup, MenuItemGroup as ItemGroup, Divider, /** @private Only used for antd internal. Do not use in your production. */
useFullPath };
var ExportMenu = Menu;
ExportMenu.Item = MenuItem;
ExportMenu.SubMenu = SubMenu;
ExportMenu.ItemGroup = MenuItemGroup;
ExportMenu.Divider = Divider;
export default ExportMenu;