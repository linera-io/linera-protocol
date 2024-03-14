"use client";

import React, { useContext } from 'react';
import { PanelPresetsContext } from '../context';
import ColorPresets from './ColorPresets';
const PanelPresets = () => {
  const {
    prefixCls,
    value,
    presets,
    onChange
  } = useContext(PanelPresetsContext);
  return Array.isArray(presets) ? ( /*#__PURE__*/React.createElement(ColorPresets, {
    value: value,
    presets: presets,
    prefixCls: prefixCls,
    onChange: onChange
  })) : null;
};
export default PanelPresets;