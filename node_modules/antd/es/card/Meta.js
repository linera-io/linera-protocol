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
const Meta = props => {
  const {
      prefixCls: customizePrefixCls,
      className,
      avatar,
      title,
      description
    } = props,
    others = __rest(props, ["prefixCls", "className", "avatar", "title", "description"]);
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('card', customizePrefixCls);
  const classString = classNames(`${prefixCls}-meta`, className);
  const avatarDom = avatar ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-avatar`
  }, avatar)) : null;
  const titleDom = title ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-title`
  }, title)) : null;
  const descriptionDom = description ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-description`
  }, description)) : null;
  const MetaDetail = titleDom || descriptionDom ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-detail`
  }, titleDom, descriptionDom)) : null;
  return /*#__PURE__*/React.createElement("div", Object.assign({}, others, {
    className: classString
  }), avatarDom, MetaDetail);
};
export default Meta;