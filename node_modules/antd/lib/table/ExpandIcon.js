"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
function renderExpandIcon(locale) {
  return function expandIcon(_ref) {
    let {
      prefixCls,
      onExpand,
      record,
      expanded,
      expandable
    } = _ref;
    const iconPrefix = `${prefixCls}-row-expand-icon`;
    return /*#__PURE__*/React.createElement("button", {
      type: "button",
      onClick: e => {
        onExpand(record, e);
        e.stopPropagation();
      },
      className: (0, _classnames.default)(iconPrefix, {
        [`${iconPrefix}-spaced`]: !expandable,
        [`${iconPrefix}-expanded`]: expandable && expanded,
        [`${iconPrefix}-collapsed`]: expandable && !expanded
      }),
      "aria-label": expanded ? locale.collapse : locale.expand,
      "aria-expanded": expanded
    });
  };
}
var _default = exports.default = renderExpandIcon;