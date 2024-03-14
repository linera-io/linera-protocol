"use client";

import React, { useEffect, useState } from 'react';
import { generateColor } from '../util';
import ColorSteppers from './ColorSteppers';
const ColorRgbInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorRgbInputPrefixCls = `${prefixCls}-rgb-input`;
  const [rgbValue, setRgbValue] = useState(generateColor(value || '#000'));
  // Update step value
  useEffect(() => {
    if (value) {
      setRgbValue(value);
    }
  }, [value]);
  const handleRgbChange = (step, type) => {
    const rgb = rgbValue.toRgb();
    rgb[type] = step || 0;
    const genColor = generateColor(rgb);
    if (!value) {
      setRgbValue(genColor);
    }
    onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
  };
  return /*#__PURE__*/React.createElement("div", {
    className: colorRgbInputPrefixCls
  }, /*#__PURE__*/React.createElement(ColorSteppers, {
    max: 255,
    min: 0,
    value: Number(rgbValue.toRgb().r),
    prefixCls: prefixCls,
    className: colorRgbInputPrefixCls,
    onChange: step => handleRgbChange(Number(step), 'r')
  }), /*#__PURE__*/React.createElement(ColorSteppers, {
    max: 255,
    min: 0,
    value: Number(rgbValue.toRgb().g),
    prefixCls: prefixCls,
    className: colorRgbInputPrefixCls,
    onChange: step => handleRgbChange(Number(step), 'g')
  }), /*#__PURE__*/React.createElement(ColorSteppers, {
    max: 255,
    min: 0,
    value: Number(rgbValue.toRgb().b),
    prefixCls: prefixCls,
    className: colorRgbInputPrefixCls,
    onChange: step => handleRgbChange(Number(step), 'b')
  }));
};
export default ColorRgbInput;