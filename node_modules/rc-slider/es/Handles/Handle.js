import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["prefixCls", "value", "valueIndex", "onStartMove", "style", "render", "dragging", "onOffsetChange", "onChangeComplete"];
import cls from 'classnames';
import KeyCode from "rc-util/es/KeyCode";
import * as React from 'react';
import SliderContext from "../context";
import { getDirectionStyle, getIndex } from "../util";
var Handle = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _cls, _getIndex;
  var prefixCls = props.prefixCls,
    value = props.value,
    valueIndex = props.valueIndex,
    onStartMove = props.onStartMove,
    style = props.style,
    render = props.render,
    dragging = props.dragging,
    onOffsetChange = props.onOffsetChange,
    onChangeComplete = props.onChangeComplete,
    restProps = _objectWithoutProperties(props, _excluded);
  var _React$useContext = React.useContext(SliderContext),
    min = _React$useContext.min,
    max = _React$useContext.max,
    direction = _React$useContext.direction,
    disabled = _React$useContext.disabled,
    keyboard = _React$useContext.keyboard,
    range = _React$useContext.range,
    tabIndex = _React$useContext.tabIndex,
    ariaLabelForHandle = _React$useContext.ariaLabelForHandle,
    ariaLabelledByForHandle = _React$useContext.ariaLabelledByForHandle,
    ariaValueTextFormatterForHandle = _React$useContext.ariaValueTextFormatterForHandle,
    styles = _React$useContext.styles,
    classNames = _React$useContext.classNames;
  var handlePrefixCls = "".concat(prefixCls, "-handle");

  // ============================ Events ============================
  var onInternalStartMove = function onInternalStartMove(e) {
    if (!disabled) {
      onStartMove(e, valueIndex);
    }
  };

  // =========================== Keyboard ===========================
  var onKeyDown = function onKeyDown(e) {
    if (!disabled && keyboard) {
      var offset = null;

      // Change the value
      switch (e.which || e.keyCode) {
        case KeyCode.LEFT:
          offset = direction === 'ltr' || direction === 'btt' ? -1 : 1;
          break;
        case KeyCode.RIGHT:
          offset = direction === 'ltr' || direction === 'btt' ? 1 : -1;
          break;

        // Up is plus
        case KeyCode.UP:
          offset = direction !== 'ttb' ? 1 : -1;
          break;

        // Down is minus
        case KeyCode.DOWN:
          offset = direction !== 'ttb' ? -1 : 1;
          break;
        case KeyCode.HOME:
          offset = 'min';
          break;
        case KeyCode.END:
          offset = 'max';
          break;
        case KeyCode.PAGE_UP:
          offset = 2;
          break;
        case KeyCode.PAGE_DOWN:
          offset = -2;
          break;
      }
      if (offset !== null) {
        e.preventDefault();
        onOffsetChange(offset, valueIndex);
      }
    }
  };
  var handleKeyUp = function handleKeyUp(e) {
    switch (e.which || e.keyCode) {
      case KeyCode.LEFT:
      case KeyCode.RIGHT:
      case KeyCode.UP:
      case KeyCode.DOWN:
      case KeyCode.HOME:
      case KeyCode.END:
      case KeyCode.PAGE_UP:
      case KeyCode.PAGE_DOWN:
        onChangeComplete === null || onChangeComplete === void 0 || onChangeComplete();
        break;
    }
  };

  // ============================ Offset ============================
  var positionStyle = getDirectionStyle(direction, value, min, max);

  // ============================ Render ============================
  var handleNode = /*#__PURE__*/React.createElement("div", _extends({
    ref: ref,
    className: cls(handlePrefixCls, (_cls = {}, _defineProperty(_cls, "".concat(handlePrefixCls, "-").concat(valueIndex + 1), range), _defineProperty(_cls, "".concat(handlePrefixCls, "-dragging"), dragging), _cls), classNames.handle),
    style: _objectSpread(_objectSpread(_objectSpread({}, positionStyle), style), styles.handle),
    onMouseDown: onInternalStartMove,
    onTouchStart: onInternalStartMove,
    onKeyDown: onKeyDown,
    onKeyUp: handleKeyUp,
    tabIndex: disabled ? null : getIndex(tabIndex, valueIndex),
    role: "slider",
    "aria-valuemin": min,
    "aria-valuemax": max,
    "aria-valuenow": value,
    "aria-disabled": disabled,
    "aria-label": getIndex(ariaLabelForHandle, valueIndex),
    "aria-labelledby": getIndex(ariaLabelledByForHandle, valueIndex),
    "aria-valuetext": (_getIndex = getIndex(ariaValueTextFormatterForHandle, valueIndex)) === null || _getIndex === void 0 ? void 0 : _getIndex(value),
    "aria-orientation": direction === 'ltr' || direction === 'rtl' ? 'horizontal' : 'vertical'
  }, restProps));

  // Customize
  if (render) {
    handleNode = render(handleNode, {
      index: valueIndex,
      prefixCls: prefixCls,
      value: value,
      dragging: dragging
    });
  }
  return handleNode;
});
if (process.env.NODE_ENV !== 'production') {
  Handle.displayName = 'Handle';
}
export default Handle;