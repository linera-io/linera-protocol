"use client";

import SearchOutlined from "@ant-design/icons/es/icons/SearchOutlined";
import * as React from 'react';
import Input from '../input';
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
  return /*#__PURE__*/React.createElement(Input, {
    placeholder: placeholder,
    className: prefixCls,
    value: value,
    onChange: handleChange,
    disabled: disabled,
    allowClear: true,
    prefix: /*#__PURE__*/React.createElement(SearchOutlined, null)
  });
};
if (process.env.NODE_ENV !== 'production') {
  Search.displayName = 'Search';
}
export default Search;