"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import { ItemGroup } from 'rc-menu';
import * as React from 'react';
import MenuDivider from '../MenuDivider';
import MenuItem from '../MenuItem';
import SubMenu from '../SubMenu';
function convertItemsToNodes(list) {
  return (list || []).map((opt, index) => {
    if (opt && typeof opt === 'object') {
      const _a = opt,
        {
          label,
          children,
          key,
          type
        } = _a,
        restProps = __rest(_a, ["label", "children", "key", "type"]);
      const mergedKey = key !== null && key !== void 0 ? key : `tmp-${index}`;
      // MenuItemGroup & SubMenuItem
      if (children || type === 'group') {
        if (type === 'group') {
          // Group
          return /*#__PURE__*/React.createElement(ItemGroup, Object.assign({
            key: mergedKey
          }, restProps, {
            title: label
          }), convertItemsToNodes(children));
        }
        // Sub Menu
        return /*#__PURE__*/React.createElement(SubMenu, Object.assign({
          key: mergedKey
        }, restProps, {
          title: label
        }), convertItemsToNodes(children));
      }
      // MenuItem & Divider
      if (type === 'divider') {
        return /*#__PURE__*/React.createElement(MenuDivider, Object.assign({
          key: mergedKey
        }, restProps));
      }
      return /*#__PURE__*/React.createElement(MenuItem, Object.assign({
        key: mergedKey
      }, restProps), label);
    }
    return null;
  }).filter(opt => opt);
}
// FIXME: Move logic here in v5
/**
 * We simply convert `items` to ReactNode for reuse origin component logic. But we need move all the
 * logic from component into this hooks when in v5
 */
export default function useItems(items) {
  return React.useMemo(() => {
    if (!items) {
      return items;
    }
    return convertItemsToNodes(items);
  }, [items]);
}