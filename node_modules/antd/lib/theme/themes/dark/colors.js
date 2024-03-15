"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.generateNeutralColorPalettes = exports.generateColorPalettes = void 0;
var _colors = require("@ant-design/colors");
var _colorAlgorithm = require("./colorAlgorithm");
const generateColorPalettes = baseColor => {
  const colors = (0, _colors.generate)(baseColor, {
    theme: 'dark'
  });
  return {
    1: colors[0],
    2: colors[1],
    3: colors[2],
    4: colors[3],
    5: colors[6],
    6: colors[5],
    7: colors[4],
    8: colors[6],
    9: colors[5],
    10: colors[4]
    // 8: colors[9],
    // 9: colors[8],
    // 10: colors[7],
  };
};
exports.generateColorPalettes = generateColorPalettes;
const generateNeutralColorPalettes = (bgBaseColor, textBaseColor) => {
  const colorBgBase = bgBaseColor || '#000';
  const colorTextBase = textBaseColor || '#fff';
  return {
    colorBgBase,
    colorTextBase,
    colorText: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.85),
    colorTextSecondary: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.65),
    colorTextTertiary: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.45),
    colorTextQuaternary: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.25),
    colorFill: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.18),
    colorFillSecondary: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.12),
    colorFillTertiary: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.08),
    colorFillQuaternary: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.04),
    colorBgElevated: (0, _colorAlgorithm.getSolidColor)(colorBgBase, 12),
    colorBgContainer: (0, _colorAlgorithm.getSolidColor)(colorBgBase, 8),
    colorBgLayout: (0, _colorAlgorithm.getSolidColor)(colorBgBase, 0),
    colorBgSpotlight: (0, _colorAlgorithm.getSolidColor)(colorBgBase, 26),
    colorBgBlur: (0, _colorAlgorithm.getAlphaColor)(colorTextBase, 0.04),
    colorBorder: (0, _colorAlgorithm.getSolidColor)(colorBgBase, 26),
    colorBorderSecondary: (0, _colorAlgorithm.getSolidColor)(colorBgBase, 19)
  };
};
exports.generateNeutralColorPalettes = generateNeutralColorPalettes;