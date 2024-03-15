"use client";

import React, { useEffect, useState } from 'react';
import { generateColor, getAlphaColor } from '../util';
import ColorSteppers from './ColorSteppers';
const ColorAlphaInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorAlphaInputPrefixCls = `${prefixCls}-alpha-input`;
  const [alphaValue, setAlphaValue] = useState(generateColor(value || '#000'));
  // Update step value
  useEffect(() => {
    if (value) {
      setAlphaValue(value);
    }
  }, [value]);
  const handleAlphaChange = step => {
    const hsba = alphaValue.toHsb();
    hsba.a = (step || 0) / 100;
    const genColor = generateColor(hsba);
    if (!value) {
      setAlphaValue(genColor);
    }
    onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
  };
  return /*#__PURE__*/React.createElement(ColorSteppers, {
    value: getAlphaColor(alphaValue),
    prefixCls: prefixCls,
    formatter: step => `${step}%`,
    className: colorAlphaInputPrefixCls,
    onChange: handleAlphaChange
  });
};
export default ColorAlphaInput;