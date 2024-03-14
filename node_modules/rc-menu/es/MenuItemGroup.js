import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["className", "title", "eventKey", "children"],
  _excluded2 = ["children"];
import classNames from 'classnames';
import omit from "rc-util/es/omit";
import * as React from 'react';
import { MenuContext } from "./context/MenuContext";
import { useFullPath, useMeasure } from "./context/PathContext";
import { parseChildren } from "./utils/commonUtil";
var InternalMenuItemGroup = function InternalMenuItemGroup(_ref) {
  var className = _ref.className,
    title = _ref.title,
    eventKey = _ref.eventKey,
    children = _ref.children,
    restProps = _objectWithoutProperties(_ref, _excluded);
  var _React$useContext = React.useContext(MenuContext),
    prefixCls = _React$useContext.prefixCls;
  var groupPrefixCls = "".concat(prefixCls, "-item-group");
  return /*#__PURE__*/React.createElement("li", _extends({
    role: "presentation"
  }, restProps, {
    onClick: function onClick(e) {
      return e.stopPropagation();
    },
    className: classNames(groupPrefixCls, className)
  }), /*#__PURE__*/React.createElement("div", {
    role: "presentation",
    className: "".concat(groupPrefixCls, "-title"),
    title: typeof title === 'string' ? title : undefined
  }, title), /*#__PURE__*/React.createElement("ul", {
    role: "group",
    className: "".concat(groupPrefixCls, "-list")
  }, children));
};
export default function MenuItemGroup(_ref2) {
  var children = _ref2.children,
    props = _objectWithoutProperties(_ref2, _excluded2);
  var connectedKeyPath = useFullPath(props.eventKey);
  var childList = parseChildren(children, connectedKeyPath);
  var measure = useMeasure();
  if (measure) {
    return childList;
  }
  return /*#__PURE__*/React.createElement(InternalMenuItemGroup, omit(props, ['warnKey']), childList);
}