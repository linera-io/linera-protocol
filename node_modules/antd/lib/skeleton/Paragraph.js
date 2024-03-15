"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
const Paragraph = props => {
  const getWidth = index => {
    const {
      width,
      rows = 2
    } = props;
    if (Array.isArray(width)) {
      return width[index];
    }
    // last paragraph
    if (rows - 1 === index) {
      return width;
    }
    return undefined;
  };
  const {
    prefixCls,
    className,
    style,
    rows
  } = props;
  const rowList = (0, _toConsumableArray2.default)(Array(rows)).map((_, index) => (
  /*#__PURE__*/
  // eslint-disable-next-line react/no-array-index-key
  React.createElement("li", {
    key: index,
    style: {
      width: getWidth(index)
    }
  })));
  return /*#__PURE__*/React.createElement("ul", {
    className: (0, _classnames.default)(prefixCls, className),
    style: style
  }, rowList);
};
var _default = exports.default = Paragraph;