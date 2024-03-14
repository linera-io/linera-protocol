"use client";

import classNames from 'classnames';
import omit from "rc-util/es/omit";
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import Element from './Element';
import useStyle from './style';
const SkeletonButton = props => {
  const {
    prefixCls: customizePrefixCls,
    className,
    rootClassName,
    active,
    block = false,
    size = 'default'
  } = props;
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('skeleton', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const otherProps = omit(props, ['prefixCls']);
  const cls = classNames(prefixCls, `${prefixCls}-element`, {
    [`${prefixCls}-active`]: active,
    [`${prefixCls}-block`]: block
  }, className, rootClassName, hashId, cssVarCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: cls
  }, /*#__PURE__*/React.createElement(Element, Object.assign({
    prefixCls: `${prefixCls}-button`,
    size: size
  }, otherProps))));
};
export default SkeletonButton;