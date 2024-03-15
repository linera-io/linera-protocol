import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import React from 'react';
var Palette = function Palette(_ref) {
  var children = _ref.children,
    style = _ref.style,
    prefixCls = _ref.prefixCls;
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-palette"),
    style: _objectSpread({
      position: 'relative'
    }, style)
  }, children);
};
export default Palette;