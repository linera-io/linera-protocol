import _typeof from "@babel/runtime/helpers/esm/typeof";
import React from "react";
export function getClearIcon(prefixCls, allowClear, clearIcon) {
  var mergedClearIcon = _typeof(allowClear) === "object" ? allowClear.clearIcon : clearIcon;
  return mergedClearIcon || /*#__PURE__*/React.createElement("span", {
    className: "".concat(prefixCls, "-clear-btn")
  });
}