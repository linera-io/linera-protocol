import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import React, { useRef } from 'react';
import useColorDrag from "../hooks/useColorDrag";
import { calculateColor, calculateOffset } from "../util";
import Palette from "./Palette";
import { useEvent } from 'rc-util';
import Gradient from "./Gradient";
import Handler from "./Handler";
import Transform from "./Transform";
var Slider = function Slider(_ref) {
  var gradientColors = _ref.gradientColors,
    direction = _ref.direction,
    _ref$type = _ref.type,
    type = _ref$type === void 0 ? 'hue' : _ref$type,
    color = _ref.color,
    value = _ref.value,
    onChange = _ref.onChange,
    onChangeComplete = _ref.onChangeComplete,
    disabled = _ref.disabled,
    prefixCls = _ref.prefixCls;
  var sliderRef = useRef();
  var transformRef = useRef();
  var colorRef = useRef(color);
  var onDragChange = useEvent(function (offsetValue) {
    var calcColor = calculateColor({
      offset: offsetValue,
      targetRef: transformRef,
      containerRef: sliderRef,
      color: color,
      type: type
    });
    colorRef.current = calcColor;
    onChange(calcColor);
  });
  var _useColorDrag = useColorDrag({
      color: color,
      targetRef: transformRef,
      containerRef: sliderRef,
      calculate: function calculate(containerRef) {
        return calculateOffset(containerRef, transformRef, color, type);
      },
      onDragChange: onDragChange,
      onDragChangeComplete: function onDragChangeComplete() {
        onChangeComplete === null || onChangeComplete === void 0 || onChangeComplete(colorRef.current, type);
      },
      direction: 'x',
      disabledDrag: disabled
    }),
    _useColorDrag2 = _slicedToArray(_useColorDrag, 2),
    offset = _useColorDrag2[0],
    dragStartHandle = _useColorDrag2[1];
  return /*#__PURE__*/React.createElement("div", {
    ref: sliderRef,
    className: classNames("".concat(prefixCls, "-slider"), "".concat(prefixCls, "-slider-").concat(type)),
    onMouseDown: dragStartHandle,
    onTouchStart: dragStartHandle
  }, /*#__PURE__*/React.createElement(Palette, {
    prefixCls: prefixCls
  }, /*#__PURE__*/React.createElement(Transform, {
    offset: offset,
    ref: transformRef
  }, /*#__PURE__*/React.createElement(Handler, {
    size: "small",
    color: value,
    prefixCls: prefixCls
  })), /*#__PURE__*/React.createElement(Gradient, {
    colors: gradientColors,
    direction: direction,
    type: type,
    prefixCls: prefixCls
  })));
};
export default Slider;