"use client";

import * as React from 'react';
import classNames from 'classnames';
import { isPresetColor } from '../_util/colors';
import { ConfigContext } from '../config-provider';
import useStyle from './style/ribbon';
const Ribbon = props => {
  const {
    className,
    prefixCls: customizePrefixCls,
    style,
    color,
    children,
    text,
    placement = 'end',
    rootClassName
  } = props;
  const {
    getPrefixCls,
    direction
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('ribbon', customizePrefixCls);
  const wrapperCls = `${prefixCls}-wrapper`;
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, wrapperCls);
  const colorInPreset = isPresetColor(color, false);
  const ribbonCls = classNames(prefixCls, `${prefixCls}-placement-${placement}`, {
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-color-${color}`]: colorInPreset
  }, className);
  const colorStyle = {};
  const cornerColorStyle = {};
  if (color && !colorInPreset) {
    colorStyle.background = color;
    cornerColorStyle.color = color;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: classNames(wrapperCls, rootClassName, hashId, cssVarCls)
  }, children, /*#__PURE__*/React.createElement("div", {
    className: classNames(ribbonCls, hashId),
    style: Object.assign(Object.assign({}, colorStyle), style)
  }, /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-text`
  }, text), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-corner`,
    style: cornerColorStyle
  }))));
};
if (process.env.NODE_ENV !== 'production') {
  Ribbon.displayName = 'Ribbon';
}
export default Ribbon;