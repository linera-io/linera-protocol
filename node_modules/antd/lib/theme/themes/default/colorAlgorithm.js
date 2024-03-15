"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getSolidColor = exports.getAlphaColor = void 0;
var _tinycolor = require("@ctrl/tinycolor");
const getAlphaColor = (baseColor, alpha) => new _tinycolor.TinyColor(baseColor).setAlpha(alpha).toRgbString();
exports.getAlphaColor = getAlphaColor;
const getSolidColor = (baseColor, brightness) => {
  const instance = new _tinycolor.TinyColor(baseColor);
  return instance.darken(brightness).toHexString();
};
exports.getSolidColor = getSolidColor;