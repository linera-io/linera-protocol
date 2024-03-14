"use client";

import * as React from 'react';
import Select from '../select';
const MiniSelect = props => /*#__PURE__*/React.createElement(Select, Object.assign({}, props, {
  showSearch: true,
  size: "small"
}));
const MiddleSelect = props => /*#__PURE__*/React.createElement(Select, Object.assign({}, props, {
  showSearch: true,
  size: "middle"
}));
MiniSelect.Option = Select.Option;
MiddleSelect.Option = Select.Option;
export { MiniSelect, MiddleSelect };