"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _tooltip = _interopRequireDefault(require("../../tooltip"));
const EllipsisTooltip = _ref => {
  let {
    enableEllipsis,
    isEllipsis,
    children,
    tooltipProps
  } = _ref;
  if (!(tooltipProps === null || tooltipProps === void 0 ? void 0 : tooltipProps.title) || !enableEllipsis) {
    return children;
  }
  return /*#__PURE__*/React.createElement(_tooltip.default, Object.assign({
    open: isEllipsis ? undefined : false
  }, tooltipProps), children);
};
if (process.env.NODE_ENV !== 'production') {
  EllipsisTooltip.displayName = 'EllipsisTooltip';
}
var _default = exports.default = EllipsisTooltip;