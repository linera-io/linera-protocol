"use client";

import DotChartOutlined from "@ant-design/icons/es/icons/DotChartOutlined";
import classNames from 'classnames';
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import useStyle from './style';
const SkeletonNode = props => {
  const {
    prefixCls: customizePrefixCls,
    className,
    rootClassName,
    style,
    active,
    children
  } = props;
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('skeleton', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const cls = classNames(prefixCls, `${prefixCls}-element`, {
    [`${prefixCls}-active`]: active
  }, hashId, className, rootClassName, cssVarCls);
  const content = children !== null && children !== void 0 ? children : /*#__PURE__*/React.createElement(DotChartOutlined, null);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: cls
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames(`${prefixCls}-image`, className),
    style: style
  }, content)));
};
export default SkeletonNode;