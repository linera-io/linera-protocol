"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import React from 'react';
import Divider from '../divider';
import PanelPicker from './components/PanelPicker';
import PanelPresets from './components/PanelPresets';
import { PanelPickerProvider, PanelPresetsProvider } from './context';
const ColorPickerPanel = props => {
  const {
      prefixCls,
      presets,
      panelRender,
      color,
      onChange,
      onClear
    } = props,
    injectProps = __rest(props, ["prefixCls", "presets", "panelRender", "color", "onChange", "onClear"]);
  const colorPickerPanelPrefixCls = `${prefixCls}-inner`;
  // ==== Inject props ===
  const panelPickerProps = Object.assign({
    prefixCls,
    value: color,
    onChange,
    onClear
  }, injectProps);
  const panelPresetsProps = React.useMemo(() => ({
    prefixCls,
    value: color,
    presets,
    onChange
  }), [prefixCls, color, presets, onChange]);
  // ====================== Render ======================
  const innerPanel = /*#__PURE__*/React.createElement("div", {
    className: `${colorPickerPanelPrefixCls}-content`
  }, /*#__PURE__*/React.createElement(PanelPicker, null), Array.isArray(presets) && /*#__PURE__*/React.createElement(Divider, null), /*#__PURE__*/React.createElement(PanelPresets, null));
  return /*#__PURE__*/React.createElement(PanelPickerProvider, {
    value: panelPickerProps
  }, /*#__PURE__*/React.createElement(PanelPresetsProvider, {
    value: panelPresetsProps
  }, /*#__PURE__*/React.createElement("div", {
    className: colorPickerPanelPrefixCls
  }, typeof panelRender === 'function' ? panelRender(innerPanel, {
    components: {
      Picker: PanelPicker,
      Presets: PanelPresets
    }
  }) : innerPanel)));
};
if (process.env.NODE_ENV !== 'production') {
  ColorPickerPanel.displayName = 'ColorPickerPanel';
}
export default ColorPickerPanel;