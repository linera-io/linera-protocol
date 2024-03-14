"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import classNames from 'classnames';
import * as React from 'react';
import { ConfigContext } from '../config-provider';
const Grid = _a => {
  var {
      prefixCls,
      className,
      hoverable = true
    } = _a,
    props = __rest(_a, ["prefixCls", "className", "hoverable"]);
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefix = getPrefixCls('card', prefixCls);
  const classString = classNames(`${prefix}-grid`, className, {
    [`${prefix}-grid-hoverable`]: hoverable
  });
  return /*#__PURE__*/React.createElement("div", Object.assign({}, props, {
    className: classString
  }));
};
export default Grid;