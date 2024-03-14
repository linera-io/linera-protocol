import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import classNames from 'classnames';
import CSSMotion from 'rc-motion';
export default function Mask(props) {
  var prefixCls = props.prefixCls,
    style = props.style,
    visible = props.visible,
    maskProps = props.maskProps,
    motionName = props.motionName,
    className = props.className;
  return /*#__PURE__*/React.createElement(CSSMotion, {
    key: "mask",
    visible: visible,
    motionName: motionName,
    leavedClassName: "".concat(prefixCls, "-mask-hidden")
  }, function (_ref, ref) {
    var motionClassName = _ref.className,
      motionStyle = _ref.style;
    return /*#__PURE__*/React.createElement("div", _extends({
      ref: ref,
      style: _objectSpread(_objectSpread({}, motionStyle), style),
      className: classNames("".concat(prefixCls, "-mask"), motionClassName, className)
    }, maskProps));
  });
}