"use client";

import React, { memo } from 'react';
import FileTextOutlined from "@ant-design/icons/es/icons/FileTextOutlined";
import classNames from 'classnames';
const FloatButtonContent = props => {
  const {
    icon,
    description,
    prefixCls,
    className
  } = props;
  const defaultElement = /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-icon`
  }, /*#__PURE__*/React.createElement(FileTextOutlined, null));
  return /*#__PURE__*/React.createElement("div", {
    onClick: props.onClick,
    onFocus: props.onFocus,
    onMouseEnter: props.onMouseEnter,
    onMouseLeave: props.onMouseLeave,
    className: classNames(className, `${prefixCls}-content`)
  }, icon || description ? ( /*#__PURE__*/React.createElement(React.Fragment, null, icon && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-icon`
  }, icon), description && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-description`
  }, description))) : defaultElement);
};
export default /*#__PURE__*/memo(FloatButtonContent);