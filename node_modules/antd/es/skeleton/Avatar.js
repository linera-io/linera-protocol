"use client";

import classNames from 'classnames';
import omit from "rc-util/es/omit";
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import Element from './Element';
import useStyle from './style';
const SkeletonAvatar = props => {
  const {
    prefixCls: customizePrefixCls,
    className,
    rootClassName,
    active,
    shape = 'circle',
    size = 'default'
  } = props;
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('skeleton', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const otherProps = omit(props, ['prefixCls', 'className']);
  const cls = classNames(prefixCls, `${prefixCls}-element`, {
    [`${prefixCls}-active`]: active
  }, className, rootClassName, hashId, cssVarCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: cls
  }, /*#__PURE__*/React.createElement(Element, Object.assign({
    prefixCls: `${prefixCls}-avatar`,
    shape: shape,
    size: size
  }, otherProps))));
};
export default SkeletonAvatar;