import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
export default function Icon(_ref) {
  var icon = _ref.icon,
    props = _ref.props,
    children = _ref.children;
  var iconNode;
  if (icon === null || icon === false) {
    return null;
  }
  if (typeof icon === 'function') {
    iconNode = /*#__PURE__*/React.createElement(icon, _objectSpread({}, props));
  } else if (typeof icon !== "boolean") {
    // Compatible for origin definition
    iconNode = icon;
  }
  return iconNode || children || null;
}