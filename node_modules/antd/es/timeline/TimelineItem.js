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
const TimelineItem = _a => {
  var {
      prefixCls: customizePrefixCls,
      className,
      color = 'blue',
      dot,
      pending = false,
      position /** Dead, but do not pass in <li {...omit()} */,
      label,
      children
    } = _a,
    restProps = __rest(_a, ["prefixCls", "className", "color", "dot", "pending", "position", "label", "children"]);
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('timeline', customizePrefixCls);
  const itemClassName = classNames(`${prefixCls}-item`, {
    [`${prefixCls}-item-pending`]: pending
  }, className);
  const customColor = /blue|red|green|gray/.test(color || '') ? undefined : color;
  const dotClassName = classNames(`${prefixCls}-item-head`, {
    [`${prefixCls}-item-head-custom`]: !!dot,
    [`${prefixCls}-item-head-${color}`]: !customColor
  });
  return /*#__PURE__*/React.createElement("li", Object.assign({}, restProps, {
    className: itemClassName
  }), label && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-label`
  }, label), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-tail`
  }), /*#__PURE__*/React.createElement("div", {
    className: dotClassName,
    style: {
      borderColor: customColor,
      color: customColor
    }
  }, dot), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-content`
  }, children));
};
export default TimelineItem;