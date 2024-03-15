"use client";

import React from 'react';
import CloseCircleFilled from "@ant-design/icons/es/icons/CloseCircleFilled";
const getAllowClear = allowClear => {
  let mergedAllowClear;
  if (typeof allowClear === 'object' && (allowClear === null || allowClear === void 0 ? void 0 : allowClear.clearIcon)) {
    mergedAllowClear = allowClear;
  } else if (allowClear) {
    mergedAllowClear = {
      clearIcon: /*#__PURE__*/React.createElement(CloseCircleFilled, null)
    };
  }
  return mergedAllowClear;
};
export default getAllowClear;