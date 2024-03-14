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
import { supportNodeRef, useComposeRef } from 'rc-util';
import { NoCompactStyle } from '../space/Compact';
const OverrideContext = /*#__PURE__*/React.createContext(null);
/** @internal Only used for Dropdown component. Do not use this in your production. */
export const OverrideProvider = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      children
    } = props,
    restProps = __rest(props, ["children"]);
  const override = React.useContext(OverrideContext);
  const context = React.useMemo(() => Object.assign(Object.assign({}, override), restProps), [override, restProps.prefixCls,
  // restProps.expandIcon, Not mark as deps since this is a ReactNode
  restProps.mode, restProps.selectable, restProps.rootClassName
  // restProps.validator, Not mark as deps since this is a function
  ]);
  const canRef = supportNodeRef(children);
  const mergedRef = useComposeRef(ref, canRef ? children.ref : null);
  return /*#__PURE__*/React.createElement(OverrideContext.Provider, {
    value: context
  }, /*#__PURE__*/React.createElement(NoCompactStyle, null, canRef ? /*#__PURE__*/React.cloneElement(children, {
    ref: mergedRef
  }) : children));
});
/** @internal Only used for Dropdown component. Do not use this in your production. */
export default OverrideContext;