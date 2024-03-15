"use client";

import React, { forwardRef } from 'react';
import classNames from 'classnames';
const IconWrapper = /*#__PURE__*/forwardRef((props, ref) => {
  const {
    className,
    style,
    children,
    prefixCls
  } = props;
  const iconWrapperCls = classNames(`${prefixCls}-icon`, className);
  return /*#__PURE__*/React.createElement("span", {
    ref: ref,
    className: iconWrapperCls,
    style: style
  }, children);
});
export default IconWrapper;