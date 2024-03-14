"use client";

import React, { useEffect, useState } from 'react';
import { generateColor, getRoundNumber } from '../util';
import ColorSteppers from './ColorSteppers';
const ColorHsbInput = _ref => {
  let {
    prefixCls,
    value,
    onChange
  } = _ref;
  const colorHsbInputPrefixCls = `${prefixCls}-hsb-input`;
  const [hsbValue, setHsbValue] = useState(generateColor(value || '#000'));
  // Update step value
  useEffect(() => {
    if (value) {
      setHsbValue(value);
    }
  }, [value]);
  const handleHsbChange = (step, type) => {
    const hsb = hsbValue.toHsb();
    hsb[type] = type === 'h' ? step : (step || 0) / 100;
    const genColor = generateColor(hsb);
    if (!value) {
      setHsbValue(genColor);
    }
    onChange === null || onChange === void 0 ? void 0 : onChange(genColor);
  };
  return /*#__PURE__*/React.createElement("div", {
    className: colorHsbInputPrefixCls
  }, /*#__PURE__*/React.createElement(ColorSteppers, {
    max: 360,
    min: 0,
    value: Number(hsbValue.toHsb().h),
    prefixCls: prefixCls,
    className: colorHsbInputPrefixCls,
    formatter: step => getRoundNumber(step || 0).toString(),
    onChange: step => handleHsbChange(Number(step), 'h')
  }), /*#__PURE__*/React.createElement(ColorSteppers, {
    max: 100,
    min: 0,
    value: Number(hsbValue.toHsb().s) * 100,
    prefixCls: prefixCls,
    className: colorHsbInputPrefixCls,
    formatter: step => `${getRoundNumber(step || 0)}%`,
    onChange: step => handleHsbChange(Number(step), 's')
  }), /*#__PURE__*/React.createElement(ColorSteppers, {
    max: 100,
    min: 0,
    value: Number(hsbValue.toHsb().b) * 100,
    prefixCls: prefixCls,
    className: colorHsbInputPrefixCls,
    formatter: step => `${getRoundNumber(step || 0)}%`,
    onChange: step => handleHsbChange(Number(step), 'b')
  }));
};
export default ColorHsbInput;