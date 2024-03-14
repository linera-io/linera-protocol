"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import { RadioOptionTypeContextProvider } from './context';
import Radio from './radio';
const RadioButton = (props, ref) => {
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const {
      prefixCls: customizePrefixCls
    } = props,
    radioProps = __rest(props, ["prefixCls"]);
  const prefixCls = getPrefixCls('radio', customizePrefixCls);
  return /*#__PURE__*/React.createElement(RadioOptionTypeContextProvider, {
    value: "button"
  }, /*#__PURE__*/React.createElement(Radio, Object.assign({
    prefixCls: prefixCls
  }, radioProps, {
    type: "radio",
    ref: ref
  })));
};
export default /*#__PURE__*/React.forwardRef(RadioButton);