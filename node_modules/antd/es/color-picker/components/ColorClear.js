"use client";

import React from 'react';
import { generateColor } from '../util';
const ColorClear = _ref => {
  let {
    prefixCls,
    value,
    colorCleared,
    onChange
  } = _ref;
  const handleClick = () => {
    if (value && !colorCleared) {
      const hsba = value.toHsb();
      hsba.a = 0;
      const genColor = generateColor(hsba);
      onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
    }
  };
  return /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-clear`,
    onClick: handleClick
  });
};
export default ColorClear;