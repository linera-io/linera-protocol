import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _typeof from "@babel/runtime/helpers/esm/typeof";
var _excluded = ["label", "children", "key", "type"];
import * as React from 'react';
import MenuItemGroup from "../MenuItemGroup";
import SubMenu from "../SubMenu";
import Divider from "../Divider";
import MenuItem from "../MenuItem";
import { parseChildren } from "./commonUtil";
function convertItemsToNodes(list) {
  return (list || []).map(function (opt, index) {
    if (opt && _typeof(opt) === 'object') {
      var _ref = opt,
        label = _ref.label,
        children = _ref.children,
        key = _ref.key,
        type = _ref.type,
        restProps = _objectWithoutProperties(_ref, _excluded);
      var mergedKey = key !== null && key !== void 0 ? key : "tmp-".concat(index);

      // MenuItemGroup & SubMenuItem
      if (children || type === 'group') {
        if (type === 'group') {
          // Group
          return /*#__PURE__*/React.createElement(MenuItemGroup, _extends({
            key: mergedKey
          }, restProps, {
            title: label
          }), convertItemsToNodes(children));
        }

        // Sub Menu
        return /*#__PURE__*/React.createElement(SubMenu, _extends({
          key: mergedKey
        }, restProps, {
          title: label
        }), convertItemsToNodes(children));
      }

      // MenuItem & Divider
      if (type === 'divider') {
        return /*#__PURE__*/React.createElement(Divider, _extends({
          key: mergedKey
        }, restProps));
      }
      return /*#__PURE__*/React.createElement(MenuItem, _extends({
        key: mergedKey
      }, restProps), label);
    }
    return null;
  }).filter(function (opt) {
    return opt;
  });
}
export function parseItems(children, items, keyPath) {
  var childNodes = children;
  if (items) {
    childNodes = convertItemsToNodes(items);
  }
  return parseChildren(childNodes, keyPath);
}