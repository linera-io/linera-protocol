import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["prefixCls", "className", "containerRef"];
import classNames from 'classnames';
import * as React from 'react';
import { RefContext } from "./context";
import pickAttrs from "rc-util/es/pickAttrs";
import { useComposeRef } from "rc-util/es/ref";
var DrawerPanel = function DrawerPanel(props) {
  var prefixCls = props.prefixCls,
    className = props.className,
    containerRef = props.containerRef,
    restProps = _objectWithoutProperties(props, _excluded);
  var _React$useContext = React.useContext(RefContext),
    panelRef = _React$useContext.panel;
  var mergedRef = useComposeRef(panelRef, containerRef);

  // =============================== Render ===============================

  return /*#__PURE__*/React.createElement("div", _extends({
    className: classNames("".concat(prefixCls, "-content"), className),
    role: "dialog",
    ref: mergedRef
  }, pickAttrs(props, {
    aria: true
  }), {
    "aria-modal": "true"
  }, restProps));
};
if (process.env.NODE_ENV !== 'production') {
  DrawerPanel.displayName = 'DrawerPanel';
}
export default DrawerPanel;