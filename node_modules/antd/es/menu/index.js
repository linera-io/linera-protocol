"use client";

import * as React from 'react';
import { forwardRef, useImperativeHandle, useRef } from 'react';
import { ItemGroup } from 'rc-menu';
import { SiderContext } from '../layout/Sider';
import InternalMenu from './menu';
import MenuDivider from './MenuDivider';
import Item from './MenuItem';
import SubMenu from './SubMenu';
const Menu = /*#__PURE__*/forwardRef((props, ref) => {
  const menuRef = useRef(null);
  const context = React.useContext(SiderContext);
  useImperativeHandle(ref, () => ({
    menu: menuRef.current,
    focus: options => {
      var _a;
      (_a = menuRef.current) === null || _a === void 0 ? void 0 : _a.focus(options);
    }
  }));
  return /*#__PURE__*/React.createElement(InternalMenu, Object.assign({
    ref: menuRef
  }, props, context));
});
Menu.Item = Item;
Menu.SubMenu = SubMenu;
Menu.Divider = MenuDivider;
Menu.ItemGroup = ItemGroup;
if (process.env.NODE_ENV !== 'production') {
  Menu.displayName = 'Menu';
}
export default Menu;