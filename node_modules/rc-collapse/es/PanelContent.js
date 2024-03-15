import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classnames from 'classnames';
import React from 'react';
var PanelContent = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _classnames;
  var prefixCls = props.prefixCls,
    forceRender = props.forceRender,
    className = props.className,
    style = props.style,
    children = props.children,
    isActive = props.isActive,
    role = props.role;
  var _React$useState = React.useState(isActive || forceRender),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    rendered = _React$useState2[0],
    setRendered = _React$useState2[1];
  React.useEffect(function () {
    if (forceRender || isActive) {
      setRendered(true);
    }
  }, [forceRender, isActive]);
  if (!rendered) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    ref: ref,
    className: classnames("".concat(prefixCls, "-content"), (_classnames = {}, _defineProperty(_classnames, "".concat(prefixCls, "-content-active"), isActive), _defineProperty(_classnames, "".concat(prefixCls, "-content-inactive"), !isActive), _classnames), className),
    style: style,
    role: role
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-content-box")
  }, children));
});
PanelContent.displayName = 'PanelContent';
export default PanelContent;