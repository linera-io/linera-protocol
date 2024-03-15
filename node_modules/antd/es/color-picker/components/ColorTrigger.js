"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import { ColorBlock } from '@rc-component/color-picker';
import classNames from 'classnames';
import React, { forwardRef, useMemo } from 'react';
import { getAlphaColor } from '../util';
import ColorClear from './ColorClear';
const ColorTrigger = /*#__PURE__*/forwardRef((props, ref) => {
  const {
      color,
      prefixCls,
      open,
      colorCleared,
      disabled,
      format,
      className,
      showText
    } = props,
    rest = __rest(props, ["color", "prefixCls", "open", "colorCleared", "disabled", "format", "className", "showText"]);
  const colorTriggerPrefixCls = `${prefixCls}-trigger`;
  const containerNode = useMemo(() => colorCleared ? ( /*#__PURE__*/React.createElement(ColorClear, {
    prefixCls: prefixCls
  })) : ( /*#__PURE__*/React.createElement(ColorBlock, {
    prefixCls: prefixCls,
    color: color.toRgbString()
  })), [color, colorCleared, prefixCls]);
  const genColorString = () => {
    const hexString = color.toHexString().toUpperCase();
    const alpha = getAlphaColor(color);
    switch (format) {
      case 'rgb':
        return color.toRgbString();
      case 'hsb':
        return color.toHsbString();
      case 'hex':
      default:
        return alpha < 100 ? `${hexString.slice(0, 7)},${alpha}%` : hexString;
    }
  };
  const renderText = () => {
    if (typeof showText === 'function') {
      return showText(color);
    }
    if (showText) {
      return genColorString();
    }
  };
  return /*#__PURE__*/React.createElement("div", Object.assign({
    ref: ref,
    className: classNames(colorTriggerPrefixCls, className, {
      [`${colorTriggerPrefixCls}-active`]: open,
      [`${colorTriggerPrefixCls}-disabled`]: disabled
    })
  }, rest), containerNode, showText && /*#__PURE__*/React.createElement("div", {
    className: `${colorTriggerPrefixCls}-text`
  }, renderText()));
});
export default ColorTrigger;