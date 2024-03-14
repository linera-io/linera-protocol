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
import { useLocale } from '../locale';
import DefaultEmptyImg from './empty';
import SimpleEmptyImg from './simple';
import useStyle from './style';
const defaultEmptyImg = /*#__PURE__*/React.createElement(DefaultEmptyImg, null);
const simpleEmptyImg = /*#__PURE__*/React.createElement(SimpleEmptyImg, null);
const Empty = _a => {
  var {
      className,
      rootClassName,
      prefixCls: customizePrefixCls,
      image = defaultEmptyImg,
      description,
      children,
      imageStyle,
      style
    } = _a,
    restProps = __rest(_a, ["className", "rootClassName", "prefixCls", "image", "description", "children", "imageStyle", "style"]);
  const {
    getPrefixCls,
    direction,
    empty
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('empty', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const [locale] = useLocale('Empty');
  const des = typeof description !== 'undefined' ? description : locale === null || locale === void 0 ? void 0 : locale.description;
  const alt = typeof des === 'string' ? des : 'empty';
  let imageNode = null;
  if (typeof image === 'string') {
    imageNode = /*#__PURE__*/React.createElement("img", {
      alt: alt,
      src: image
    });
  } else {
    imageNode = image;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({
    className: classNames(hashId, cssVarCls, prefixCls, empty === null || empty === void 0 ? void 0 : empty.className, {
      [`${prefixCls}-normal`]: image === simpleEmptyImg,
      [`${prefixCls}-rtl`]: direction === 'rtl'
    }, className, rootClassName),
    style: Object.assign(Object.assign({}, empty === null || empty === void 0 ? void 0 : empty.style), style)
  }, restProps), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-image`,
    style: imageStyle
  }, imageNode), des && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-description`
  }, des), children && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-footer`
  }, children)));
};
Empty.PRESENTED_IMAGE_DEFAULT = defaultEmptyImg;
Empty.PRESENTED_IMAGE_SIMPLE = simpleEmptyImg;
if (process.env.NODE_ENV !== 'production') {
  Empty.displayName = 'Empty';
}
export default Empty;