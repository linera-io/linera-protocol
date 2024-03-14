import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["prefixCls", "style", "onStartMove", "onOffsetChange", "values", "handleRender", "draggingIndex"];
import * as React from 'react';
import Handle from "./Handle";
import { getIndex } from "../util";
var Handles = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    style = props.style,
    onStartMove = props.onStartMove,
    onOffsetChange = props.onOffsetChange,
    values = props.values,
    handleRender = props.handleRender,
    draggingIndex = props.draggingIndex,
    restProps = _objectWithoutProperties(props, _excluded);
  var handlesRef = React.useRef({});
  React.useImperativeHandle(ref, function () {
    return {
      focus: function focus(index) {
        var _handlesRef$current$i;
        (_handlesRef$current$i = handlesRef.current[index]) === null || _handlesRef$current$i === void 0 || _handlesRef$current$i.focus();
      }
    };
  });
  return /*#__PURE__*/React.createElement(React.Fragment, null, values.map(function (value, index) {
    return /*#__PURE__*/React.createElement(Handle, _extends({
      ref: function ref(node) {
        if (!node) {
          delete handlesRef.current[index];
        } else {
          handlesRef.current[index] = node;
        }
      },
      dragging: draggingIndex === index,
      prefixCls: prefixCls,
      style: getIndex(style, index),
      key: index,
      value: value,
      valueIndex: index,
      onStartMove: onStartMove,
      onOffsetChange: onOffsetChange,
      render: handleRender
    }, restProps));
  }));
});
if (process.env.NODE_ENV !== 'production') {
  Handles.displayName = 'Handles';
}
export default Handles;