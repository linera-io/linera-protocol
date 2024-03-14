"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
/**
 * Wrap of sub component which need use as Button capacity (like Icon component).
 *
 * This helps accessibility reader to tread as a interactive button to operation.
 */
import KeyCode from "rc-util/es/KeyCode";
import * as React from 'react';
const inlineStyle = {
  border: 0,
  background: 'transparent',
  padding: 0,
  lineHeight: 'inherit',
  display: 'inline-block'
};
const TransButton = /*#__PURE__*/React.forwardRef((props, ref) => {
  const onKeyDown = event => {
    const {
      keyCode
    } = event;
    if (keyCode === KeyCode.ENTER) {
      event.preventDefault();
    }
  };
  const onKeyUp = event => {
    const {
      keyCode
    } = event;
    const {
      onClick
    } = props;
    if (keyCode === KeyCode.ENTER && onClick) {
      onClick();
    }
  };
  const {
      style,
      noStyle,
      disabled
    } = props,
    restProps = __rest(props, ["style", "noStyle", "disabled"]);
  let mergedStyle = {};
  if (!noStyle) {
    mergedStyle = Object.assign({}, inlineStyle);
  }
  if (disabled) {
    mergedStyle.pointerEvents = 'none';
  }
  mergedStyle = Object.assign(Object.assign({}, mergedStyle), style);
  return /*#__PURE__*/React.createElement("div", Object.assign({
    role: "button",
    tabIndex: 0,
    ref: ref
  }, restProps, {
    onKeyDown: onKeyDown,
    onKeyUp: onKeyUp,
    style: mergedStyle
  }));
});
export default TransButton;