import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import React, { useRef } from 'react';
import useColorDrag from "../hooks/useColorDrag";
import { calculateColor, calculateOffset } from "../util";
import { useEvent } from 'rc-util';
import Handler from "./Handler";
import Palette from "./Palette";
import Transform from "./Transform";
var Picker = function Picker(_ref) {
  var color = _ref.color,
    onChange = _ref.onChange,
    prefixCls = _ref.prefixCls,
    onChangeComplete = _ref.onChangeComplete,
    disabled = _ref.disabled;
  var pickerRef = useRef();
  var transformRef = useRef();
  var colorRef = useRef(color);
  var onDragChange = useEvent(function (offsetValue) {
    var calcColor = calculateColor({
      offset: offsetValue,
      targetRef: transformRef,
      containerRef: pickerRef,
      color: color
    });
    colorRef.current = calcColor;
    onChange(calcColor);
  });
  var _useColorDrag = useColorDrag({
      color: color,
      containerRef: pickerRef,
      targetRef: transformRef,
      calculate: function calculate(containerRef) {
        return calculateOffset(containerRef, transformRef, color);
      },
      onDragChange: onDragChange,
      onDragChangeComplete: function onDragChangeComplete() {
        return onChangeComplete === null || onChangeComplete === void 0 ? void 0 : onChangeComplete(colorRef.current);
      },
      disabledDrag: disabled
    }),
    _useColorDrag2 = _slicedToArray(_useColorDrag, 2),
    offset = _useColorDrag2[0],
    dragStartHandle = _useColorDrag2[1];
  return /*#__PURE__*/React.createElement("div", {
    ref: pickerRef,
    className: "".concat(prefixCls, "-select"),
    onMouseDown: dragStartHandle,
    onTouchStart: dragStartHandle
  }, /*#__PURE__*/React.createElement(Palette, {
    prefixCls: prefixCls
  }, /*#__PURE__*/React.createElement(Transform, {
    offset: offset,
    ref: transformRef
  }, /*#__PURE__*/React.createElement(Handler, {
    color: color.toRgbString(),
    prefixCls: prefixCls
  })), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-saturation"),
    style: {
      backgroundColor: "hsl(".concat(color.toHsb().h, ",100%, 50%)"),
      backgroundImage: 'linear-gradient(0deg, #000, transparent),linear-gradient(90deg, #fff, hsla(0, 0%, 100%, 0))'
    }
  })));
};
export default Picker;