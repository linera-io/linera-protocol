"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import React from 'react';
import classNames from 'classnames';
import omit from "rc-util/es/omit";
import { isPresetSize } from '../_util/gapSize';
import { ConfigContext } from '../config-provider';
import useStyle from './style';
import createFlexClassNames from './utils';
const Flex = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      rootClassName,
      className,
      style,
      flex,
      gap,
      children,
      vertical = false,
      component: Component = 'div'
    } = props,
    othersProps = __rest(props, ["prefixCls", "rootClassName", "className", "style", "flex", "gap", "children", "vertical", "component"]);
  const {
    flex: ctxFlex,
    direction: ctxDirection,
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('flex', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const mergedVertical = vertical !== null && vertical !== void 0 ? vertical : ctxFlex === null || ctxFlex === void 0 ? void 0 : ctxFlex.vertical;
  const mergedCls = classNames(className, rootClassName, ctxFlex === null || ctxFlex === void 0 ? void 0 : ctxFlex.className, prefixCls, hashId, cssVarCls, createFlexClassNames(prefixCls, props), {
    [`${prefixCls}-rtl`]: ctxDirection === 'rtl',
    [`${prefixCls}-gap-${gap}`]: isPresetSize(gap),
    [`${prefixCls}-vertical`]: mergedVertical
  });
  const mergedStyle = Object.assign(Object.assign({}, ctxFlex === null || ctxFlex === void 0 ? void 0 : ctxFlex.style), style);
  if (flex) {
    mergedStyle.flex = flex;
  }
  if (gap && !isPresetSize(gap)) {
    mergedStyle.gap = gap;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement(Component, Object.assign({
    ref: ref,
    className: mergedCls,
    style: mergedStyle
  }, omit(othersProps, ['justify', 'wrap', 'align'])), children));
});
if (process.env.NODE_ENV !== 'production') {
  Flex.displayName = 'Flex';
}
export default Flex;