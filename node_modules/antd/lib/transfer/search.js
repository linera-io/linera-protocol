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
var _input = _interopRequireDefault(require("../input"));
const Search = props => {
  const {
    placeholder = '',
    value,
    prefixCls,
    disabled,
    onChange,
    handleClear
  } = props;
  const handleChange = React.useCallback(e => {
    onChange === null || onChange === void 0 ? void 0 : onChange(e);
    if (e.target.value === '') {
      handleClear === null || handleClear === void 0 ? void 0 : handleClear();
    }
  }, [onChange]);
  return /*#__PURE__*/React.createElement(_input.default, {
    placeholder: placeholder,
    className: prefixCls,
    value: value,
    onChange: handleChange,
    disabled: disabled,
    allowClear: true,
    prefix: /*#__PURE__*/React.createElement(_SearchOutlined.default, null)
  });
};
if (process.env.NODE_ENV !== 'production') {
  Search.displayName = 'Search';
}
var _default = exports.default = Search;