import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import classNames from 'classnames';
import { getDirectionStyle } from "../util";
import SliderContext from "../context";
export default function Dot(props) {
  var prefixCls = props.prefixCls,
    value = props.value,
    style = props.style,
    activeStyle = props.activeStyle;
  var _React$useContext = React.useContext(SliderContext),
    min = _React$useContext.min,
    max = _React$useContext.max,
    direction = _React$useContext.direction,
    included = _React$useContext.included,
    includedStart = _React$useContext.includedStart,
    includedEnd = _React$useContext.includedEnd;
  var dotClassName = "".concat(prefixCls, "-dot");
  var active = included && includedStart <= value && value <= includedEnd;

  // ============================ Offset ============================
  var mergedStyle = _objectSpread(_objectSpread({}, getDirectionStyle(direction, value, min, max)), typeof style === 'function' ? style(value) : style);
  if (active) {
    mergedStyle = _objectSpread(_objectSpread({}, mergedStyle), typeof activeStyle === 'function' ? activeStyle(value) : activeStyle);
  }
  return /*#__PURE__*/React.createElement("span", {
    className: classNames(dotClassName, _defineProperty({}, "".concat(dotClassName, "-active"), active)),
    style: mergedStyle
  });
}