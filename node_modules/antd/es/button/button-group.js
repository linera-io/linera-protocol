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
import classNames from 'classnames';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import { useToken } from '../theme/internal';
export const GroupSizeContext = /*#__PURE__*/React.createContext(undefined);
const ButtonGroup = props => {
  const {
    getPrefixCls,
    direction
  } = React.useContext(ConfigContext);
  const {
      prefixCls: customizePrefixCls,
      size,
      className
    } = props,
    others = __rest(props, ["prefixCls", "size", "className"]);
  const prefixCls = getPrefixCls('btn-group', customizePrefixCls);
  const [,, hashId] = useToken();
  let sizeCls = '';
  switch (size) {
    case 'large':
      sizeCls = 'lg';
      break;
    case 'small':
      sizeCls = 'sm';
      break;
    case 'middle':
    default:
    // Do nothing
  }
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Button.Group');
    process.env.NODE_ENV !== "production" ? warning(!size || ['large', 'small', 'middle'].includes(size), 'usage', 'Invalid prop `size`.') : void 0;
  }
  const classes = classNames(prefixCls, {
    [`${prefixCls}-${sizeCls}`]: sizeCls,
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, hashId);
  return /*#__PURE__*/React.createElement(GroupSizeContext.Provider, {
    value: size
  }, /*#__PURE__*/React.createElement("div", Object.assign({}, others, {
    className: classes
  })));
};
export default ButtonGroup;