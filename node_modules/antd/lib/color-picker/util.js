"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getRoundNumber = exports.getAlphaColor = exports.generateColor = exports.genAlphaColor = void 0;
var _color = require("./color");
const generateColor = color => {
  if (color instanceof _color.ColorFactory) {
    return color;
  }
  return new _color.ColorFactory(color);
};
exports.generateColor = generateColor;
const getRoundNumber = value => Math.round(Number(value || 0));
exports.getRoundNumber = getRoundNumber;
const getAlphaColor = color => getRoundNumber(color.toHsb().a * 100);
exports.getAlphaColor = getAlphaColor;
const genAlphaColor = (color, alpha) => {
  const hsba = color.toHsb();
  hsba.a = alpha || 1;
  return generateColor(hsba);
};
exports.genAlphaColor = genAlphaColor;