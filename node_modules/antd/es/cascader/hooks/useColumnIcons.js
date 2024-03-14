"use client";

import * as React from 'react';
import LeftOutlined from "@ant-design/icons/es/icons/LeftOutlined";
import LoadingOutlined from "@ant-design/icons/es/icons/LoadingOutlined";
import RightOutlined from "@ant-design/icons/es/icons/RightOutlined";
export default function useColumnIcons(prefixCls, rtl, expandIcon) {
  let mergedExpandIcon = expandIcon;
  if (!expandIcon) {
    mergedExpandIcon = rtl ? /*#__PURE__*/React.createElement(LeftOutlined, null) : /*#__PURE__*/React.createElement(RightOutlined, null);
  }
  const loadingIcon = /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-menu-item-loading-icon`
  }, /*#__PURE__*/React.createElement(LoadingOutlined, {
    spin: true
  }));
  return [mergedExpandIcon, loadingIcon];
}