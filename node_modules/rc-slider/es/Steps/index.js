import * as React from 'react';
import SliderContext from "../context";
import Dot from "./Dot";
export default function Steps(props) {
  var prefixCls = props.prefixCls,
    marks = props.marks,
    dots = props.dots,
    style = props.style,
    activeStyle = props.activeStyle;
  var _React$useContext = React.useContext(SliderContext),
    min = _React$useContext.min,
    max = _React$useContext.max,
    step = _React$useContext.step;
  var stepDots = React.useMemo(function () {
    var dotSet = new Set();

    // Add marks
    marks.forEach(function (mark) {
      dotSet.add(mark.value);
    });

    // Fill dots
    if (dots && step !== null) {
      var current = min;
      while (current <= max) {
        dotSet.add(current);
        current += step;
      }
    }
    return Array.from(dotSet);
  }, [min, max, step, dots, marks]);
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-step")
  }, stepDots.map(function (dotValue) {
    return /*#__PURE__*/React.createElement(Dot, {
      prefixCls: prefixCls,
      key: dotValue,
      value: dotValue,
      style: style,
      activeStyle: activeStyle
    });
  }));
}