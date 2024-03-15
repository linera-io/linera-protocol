"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _SearchOutlined = _interopRequireDefault(require("@ant-design/icons/SearchOutlined"));
var React = _interopRequireWildcard(require("react"));
var _input = _interopRequireDefault(require("../../../input"));
function FilterSearch(_ref) {
  let {
    value,
    onChange,
    filterSearch,
    tablePrefixCls,
    locale
  } = _ref;
  if (!filterSearch) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: `${tablePrefixCls}-filter-dropdown-search`
  }, /*#__PURE__*/React.createElement(_input.default, {
    prefix: /*#__PURE__*/React.createElement(_SearchOutlined.default, null),
    placeholder: locale.filterSearchPlaceholder,
    onChange: onChange,
    value: value,
    // for skip min-width of input
    htmlSize: 1,
    className: `${tablePrefixCls}-filter-dropdown-search-input`
  }));
}
var _default = exports.default = FilterSearch;