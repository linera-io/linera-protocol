"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = genPresetColor;
var _interface = require("../interface");
function genPresetColor(token, genCss) {
  return _interface.PresetColors.reduce((prev, colorKey) => {
    const lightColor = token[`${colorKey}1`];
    const lightBorderColor = token[`${colorKey}3`];
    const darkColor = token[`${colorKey}6`];
    const textColor = token[`${colorKey}7`];
    return Object.assign(Object.assign({}, prev), genCss(colorKey, {
      lightColor,
      lightBorderColor,
      darkColor,
      textColor
    }));
  }, {});
}