import _extends from "@babel/runtime/helpers/esm/extends";
import classNames from 'classnames';
import CSSMotion from 'rc-motion';
import * as React from 'react';
export default function Mask(props) {
  var prefixCls = props.prefixCls,
    open = props.open,
    zIndex = props.zIndex,
    mask = props.mask,
    motion = props.motion;
  if (!mask) {
    return null;
  }
  return /*#__PURE__*/React.createElement(CSSMotion, _extends({}, motion, {
    motionAppear: true,
    visible: open,
    removeOnLeave: true
  }), function (_ref) {
    var className = _ref.className;
    return /*#__PURE__*/React.createElement("div", {
      style: {
        zIndex: zIndex
      },
      className: classNames("".concat(prefixCls, "-mask"), className)
    });
  });
}