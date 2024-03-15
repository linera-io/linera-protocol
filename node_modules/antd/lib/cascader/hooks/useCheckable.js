"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useCheckable;
var React = _interopRequireWildcard(require("react"));
function useCheckable(cascaderPrefixCls, multiple) {
  return React.useMemo(() => multiple ? /*#__PURE__*/React.createElement("span", {
    className: `${cascaderPrefixCls}-checkbox-inner`
  }) : false, [multiple]);
}