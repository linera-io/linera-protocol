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
import { devUseWarning } from '../_util/warning';
import Base from './Base';
const TITLE_ELE_LIST = [1, 2, 3, 4, 5];
const Title = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      level = 1
    } = props,
    restProps = __rest(props, ["level"]);
  let component;
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Typography.Title');
    process.env.NODE_ENV !== "production" ? warning(TITLE_ELE_LIST.includes(level), 'usage', 'Title only accept `1 | 2 | 3 | 4 | 5` as `level` value. And `5` need 4.6.0+ version.') : void 0;
  }
  if (TITLE_ELE_LIST.includes(level)) {
    component = `h${level}`;
  } else {
    component = 'h1';
  }
  return /*#__PURE__*/React.createElement(Base, Object.assign({
    ref: ref
  }, restProps, {
    component: component
  }));
});
export default Title;