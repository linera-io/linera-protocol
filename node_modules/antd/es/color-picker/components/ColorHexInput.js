"use client";

import React, { useEffect, useState } from 'react';
import Input from '../../input';
import { toHexFormat } from '../color';
import { generateColor } from '../util';
const hexReg = /(^#[\da-f]{6}$)|(^#[\da-f]{8}$)/i;
const isHexString = hex => hexReg.test(`#${hex}`);
const ColorHexInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorHexInputPrefixCls = `${prefixCls}-hex-input`;
  const [hexValue, setHexValue] = useState(value === null || value === void 0 ? void 0 : value.toHex());
  // Update step value
  useEffect(() => {
    const hex = value === null || value === void 0 ? void 0 : value.toHex();
    if (isHexString(hex) && value) {
      setHexValue(toHexFormat(hex));
    }
  }, [value]);
  const handleHexChange = e => {
    const originValue = e.target.value;
    setHexValue(toHexFormat(originValue));
    if (isHexString(toHexFormat(originValue, true))) {
      onChange === null || onChange === void 0 ? void 0 : onChange(generateColor(originValue));
    }
  };
  return /*#__PURE__*/React.createElement(Input, {
    className: colorHexInputPrefixCls,
    value: hexValue,
    prefix: "#",
    onChange: handleHexChange,
    size: "small"
  });
};
export default ColorHexInput;