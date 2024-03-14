"use client";

import classNames from 'classnames';
import React, { useEffect, useState } from 'react';
import InputNumber from '../../input-number';
const ColorSteppers = _ref => {
  let {
    prefixCls,
    min = 0,
    max = 100,
    value,
    onChange,
    className,
    formatter
  } = _ref;
  const colorSteppersPrefixCls = `${prefixCls}-steppers`;
  const [stepValue, setStepValue] = useState(value);
  // Update step value
  useEffect(() => {
    if (!Number.isNaN(value)) {
      setStepValue(value);
    }
  }, [value]);
  return /*#__PURE__*/React.createElement(InputNumber, {
    className: classNames(colorSteppersPrefixCls, className),
    min: min,
    max: max,
    value: stepValue,
    formatter: formatter,
    size: "small",
    onChange: step => {
      if (!value) {
        setStepValue(step || 0);
      }
      onChange === null || onChange === void 0 ? void 0 : onChange(step);
    }
  });
};
export default ColorSteppers;