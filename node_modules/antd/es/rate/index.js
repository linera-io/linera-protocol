"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import StarFilled from "@ant-design/icons/es/icons/StarFilled";
import classNames from 'classnames';
import RcRate from 'rc-rate';
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import Tooltip from '../tooltip';
import useStyle from './style';
const Rate = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls,
      className,
      rootClassName,
      style,
      tooltips,
      character = /*#__PURE__*/React.createElement(StarFilled, null)
    } = props,
    rest = __rest(props, ["prefixCls", "className", "rootClassName", "style", "tooltips", "character"]);
  const characterRender = (node, _ref) => {
    let {
      index
    } = _ref;
    if (!tooltips) {
      return node;
    }
    return /*#__PURE__*/React.createElement(Tooltip, {
      title: tooltips[index]
    }, node);
  };
  const {
    getPrefixCls,
    direction,
    rate
  } = React.useContext(ConfigContext);
  const ratePrefixCls = getPrefixCls('rate', prefixCls);
  // Style
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(ratePrefixCls);
  const mergedStyle = Object.assign(Object.assign({}, rate === null || rate === void 0 ? void 0 : rate.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement(RcRate, Object.assign({
    ref: ref,
    character: character,
    characterRender: characterRender
  }, rest, {
    className: classNames(className, rootClassName, hashId, cssVarCls, rate === null || rate === void 0 ? void 0 : rate.className),
    style: mergedStyle,
    prefixCls: ratePrefixCls,
    direction: direction
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Rate.displayName = 'Rate';
}
export default Rate;