import classNames from 'classnames';
import React from 'react';
var ColorBlock = function ColorBlock(_ref) {
  var color = _ref.color,
    prefixCls = _ref.prefixCls,
    className = _ref.className,
    style = _ref.style,
    onClick = _ref.onClick;
  var colorBlockCls = "".concat(prefixCls, "-color-block");
  return /*#__PURE__*/React.createElement("div", {
    className: classNames(colorBlockCls, className),
    style: style,
    onClick: onClick
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(colorBlockCls, "-inner"),
    style: {
      background: color
    }
  }));
};
export default ColorBlock;