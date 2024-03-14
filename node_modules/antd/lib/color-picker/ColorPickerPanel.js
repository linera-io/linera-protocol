"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
var _divider = _interopRequireDefault(require("../divider"));
var _PanelPicker = _interopRequireDefault(require("./components/PanelPicker"));
var _PanelPresets = _interopRequireDefault(require("./components/PanelPresets"));
var _context = require("./context");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
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
  const panelPresetsProps = _react.default.useMemo(() => ({
    prefixCls,
    value: color,
    presets,
    onChange
  }), [prefixCls, color, presets, onChange]);
  // ====================== Render ======================
  const innerPanel = /*#__PURE__*/_react.default.createElement("div", {
    className: `${colorPickerPanelPrefixCls}-content`
  }, /*#__PURE__*/_react.default.createElement(_PanelPicker.default, null), Array.isArray(presets) && /*#__PURE__*/_react.default.createElement(_divider.default, null), /*#__PURE__*/_react.default.createElement(_PanelPresets.default, null));
  return /*#__PURE__*/_react.default.createElement(_context.PanelPickerProvider, {
    value: panelPickerProps
  }, /*#__PURE__*/_react.default.createElement(_context.PanelPresetsProvider, {
    value: panelPresetsProps
  }, /*#__PURE__*/_react.default.createElement("div", {
    className: colorPickerPanelPrefixCls
  }, typeof panelRender === 'function' ? panelRender(innerPanel, {
    components: {
      Picker: _PanelPicker.default,
      Presets: _PanelPresets.default
    }
  }) : innerPanel)));
};
if (process.env.NODE_ENV !== 'production') {
  ColorPickerPanel.displayName = 'ColorPickerPanel';
}
var _default = exports.default = ColorPickerPanel;