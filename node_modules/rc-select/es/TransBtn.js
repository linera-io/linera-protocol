import * as React from 'react';
import classNames from 'classnames';
var TransBtn = function TransBtn(props) {
  var className = props.className,
    customizeIcon = props.customizeIcon,
    customizeIconProps = props.customizeIconProps,
    children = props.children,
    _onMouseDown = props.onMouseDown,
    onClick = props.onClick;
  var icon = typeof customizeIcon === 'function' ? customizeIcon(customizeIconProps) : customizeIcon;
  return /*#__PURE__*/React.createElement("span", {
    className: className,
    onMouseDown: function onMouseDown(event) {
      event.preventDefault();
      _onMouseDown === null || _onMouseDown === void 0 || _onMouseDown(event);
    },
    style: {
      userSelect: 'none',
      WebkitUserSelect: 'none'
    },
    unselectable: "on",
    onClick: onClick,
    "aria-hidden": true
  }, icon !== undefined ? icon : /*#__PURE__*/React.createElement("span", {
    className: classNames(className.split(/\s+/).map(function (cls) {
      return "".concat(cls, "-icon");
    }))
  }, children));
};
export default TransBtn;