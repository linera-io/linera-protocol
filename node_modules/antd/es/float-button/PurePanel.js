"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
/* eslint-disable react/no-array-index-key */
import classNames from 'classnames';
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import BackTop from './BackTop';
import FloatButton, { floatButtonPrefixCls } from './FloatButton';
import FloatButtonGroup from './FloatButtonGroup';
const PureFloatButton = _a => {
  var {
      backTop
    } = _a,
    props = __rest(_a, ["backTop"]);
  return backTop ? /*#__PURE__*/React.createElement(BackTop, Object.assign({}, props, {
    visibilityHeight: 0
  })) : /*#__PURE__*/React.createElement(FloatButton, Object.assign({}, props));
};
/** @private Internal Component. Do not use in your production. */
const PurePanel = _a => {
  var {
      className,
      items
    } = _a,
    props = __rest(_a, ["className", "items"]);
  const {
    prefixCls: customizePrefixCls
  } = props;
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls(floatButtonPrefixCls, customizePrefixCls);
  const pureCls = `${prefixCls}-pure`;
  if (items) {
    return /*#__PURE__*/React.createElement(FloatButtonGroup, Object.assign({
      className: classNames(className, pureCls)
    }, props), items.map((item, index) => ( /*#__PURE__*/React.createElement(PureFloatButton, Object.assign({
      key: index
    }, item)))));
  }
  return /*#__PURE__*/React.createElement(PureFloatButton, Object.assign({
    className: classNames(className, pureCls)
  }, props));
};
export default PurePanel;