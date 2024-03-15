"use client";

import SearchOutlined from "@ant-design/icons/es/icons/SearchOutlined";
import * as React from 'react';
import Input from '../../../input';
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
  }, /*#__PURE__*/React.createElement(Input, {
    prefix: /*#__PURE__*/React.createElement(SearchOutlined, null),
    placeholder: locale.filterSearchPlaceholder,
    onChange: onChange,
    value: value,
    // for skip min-width of input
    htmlSize: 1,
    className: `${tablePrefixCls}-filter-dropdown-search-input`
  }));
}
export default FilterSearch;