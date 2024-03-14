import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import * as React from 'react';
import classNames from 'classnames';
import { getDirectionStyle } from "../util";
import SliderContext from "../context";
export default function Mark(props) {
  var prefixCls = props.prefixCls,
    style = props.style,
    children = props.children,
    value = props.value,
    _onClick = props.onClick;
  var _React$useContext = React.useContext(SliderContext),
    min = _React$useContext.min,
    max = _React$useContext.max,
    direction = _React$useContext.direction,
    includedStart = _React$useContext.includedStart,
    includedEnd = _React$useContext.includedEnd,
    included = _React$useContext.included;
  var textCls = "".concat(prefixCls, "-text");

  // ============================ Offset ============================
  var positionStyle = getDirectionStyle(direction, value, min, max);
  return /*#__PURE__*/React.createElement("span", {
    className: classNames(textCls, _defineProperty({}, "".concat(textCls, "-active"), included && includedStart <= value && value <= includedEnd)),
    style: _objectSpread(_objectSpread({}, positionStyle), style),
    onMouseDown: function onMouseDown(e) {
      e.stopPropagation();
    },
    onClick: function onClick() {
      _onClick(value);
    }
  }, children);
}