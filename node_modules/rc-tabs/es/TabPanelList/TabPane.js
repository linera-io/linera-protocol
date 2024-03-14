import classNames from 'classnames';
import * as React from 'react';
var TabPane = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    className = props.className,
    style = props.style,
    id = props.id,
    active = props.active,
    tabKey = props.tabKey,
    children = props.children;
  return /*#__PURE__*/React.createElement("div", {
    id: id && "".concat(id, "-panel-").concat(tabKey),
    role: "tabpanel",
    tabIndex: active ? 0 : -1,
    "aria-labelledby": id && "".concat(id, "-tab-").concat(tabKey),
    "aria-hidden": !active,
    style: style,
    className: classNames(prefixCls, active && "".concat(prefixCls, "-active"), className),
    ref: ref
  }, children);
});
if (process.env.NODE_ENV !== 'production') {
  TabPane.displayName = 'TabPane';
}
export default TabPane;