"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var React = _interopRequireWildcard(require("react"));
const onKeyDown = event => {
  const {
    keyCode
  } = event;
  if (keyCode === _KeyCode.default.ENTER) {
    event.stopPropagation();
  }
};
const FilterDropdownMenuWrapper = /*#__PURE__*/React.forwardRef((props, ref) => ( /*#__PURE__*/React.createElement("div", {
  className: props.className,
  onClick: e => e.stopPropagation(),
  onKeyDown: onKeyDown,
  ref: ref
}, props.children)));
if (process.env.NODE_ENV !== 'production') {
  FilterDropdownMenuWrapper.displayName = 'FilterDropdownMenuWrapper';
}
var _default = exports.default = FilterDropdownMenuWrapper;