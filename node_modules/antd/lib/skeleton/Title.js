"use strict";
"use client";

/* eslint-disable jsx-a11y/heading-has-content */
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
const Title = _ref => {
  let {
    prefixCls,
    className,
    width,
    style
  } = _ref;
  return /*#__PURE__*/React.createElement("h3", {
    className: (0, _classnames.default)(prefixCls, className),
    style: Object.assign({
      width
    }, style)
  });
};
var _default = exports.default = Title;