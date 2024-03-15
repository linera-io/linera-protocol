"use client";

import * as React from 'react';
export default function useCheckable(cascaderPrefixCls, multiple) {
  return React.useMemo(() => multiple ? /*#__PURE__*/React.createElement("span", {
    className: `${cascaderPrefixCls}-checkbox-inner`
  }) : false, [multiple]);
}