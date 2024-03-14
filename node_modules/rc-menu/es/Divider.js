import * as React from 'react';
import classNames from 'classnames';
import { MenuContext } from "./context/MenuContext";
import { useMeasure } from "./context/PathContext";
export default function Divider(_ref) {
  var className = _ref.className,
    style = _ref.style;
  var _React$useContext = React.useContext(MenuContext),
    prefixCls = _React$useContext.prefixCls;
  var measure = useMeasure();
  if (measure) {
    return null;
  }
  return /*#__PURE__*/React.createElement("li", {
    role: "separator",
    className: classNames("".concat(prefixCls, "-item-divider"), className),
    style: style
  });
}