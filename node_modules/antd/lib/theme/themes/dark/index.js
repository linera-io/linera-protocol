"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _colors = require("@ant-design/colors");
var _seed = require("../seed");
var _genColorMapToken = _interopRequireDefault(require("../shared/genColorMapToken"));
var _colors2 = require("./colors");
var _default2 = _interopRequireDefault(require("../default"));
const derivative = (token, mapToken) => {
  const colorPalettes = Object.keys(_seed.defaultPresetColors).map(colorKey => {
    const colors = (0, _colors.generate)(token[colorKey], {
      theme: 'dark'
    });
    return new Array(10).fill(1).reduce((prev, _, i) => {
      prev[`${colorKey}-${i + 1}`] = colors[i];
      prev[`${colorKey}${i + 1}`] = colors[i];
      return prev;
    }, {});
  }).reduce((prev, cur) => {
    prev = Object.assign(Object.assign({}, prev), cur);
    return prev;
  }, {});
  const mergedMapToken = mapToken !== null && mapToken !== void 0 ? mapToken : (0, _default2.default)(token);
  return Object.assign(Object.assign(Object.assign({}, mergedMapToken), colorPalettes), (0, _genColorMapToken.default)(token, {
    generateColorPalettes: _colors2.generateColorPalettes,
    generateNeutralColorPalettes: _colors2.generateNeutralColorPalettes
  }));
};
var _default = exports.default = derivative;