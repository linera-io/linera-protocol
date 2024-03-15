"use client";

import KeyCode from "rc-util/es/KeyCode";
import * as React from 'react';
const onKeyDown = event => {
  const {
    keyCode
  } = event;
  if (keyCode === KeyCode.ENTER) {
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
export default FilterDropdownMenuWrapper;