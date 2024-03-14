"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = derivative;
var _colors = require("@ant-design/colors");
var _genControlHeight = _interopRequireDefault(require("../shared/genControlHeight"));
var _genSizeMapToken = _interopRequireDefault(require("../shared/genSizeMapToken"));
var _seed = require("../seed");
var _genColorMapToken = _interopRequireDefault(require("../shared/genColorMapToken"));
var _genCommonMapToken = _interopRequireDefault(require("../shared/genCommonMapToken"));
var _colors2 = require("./colors");
var _genFontMapToken = _interopRequireDefault(require("../shared/genFontMapToken"));
function derivative(token) {
  const colorPalettes = Object.keys(_seed.defaultPresetColors).map(colorKey => {
    const colors = (0, _colors.generate)(token[colorKey]);
    return new Array(10).fill(1).reduce((prev, _, i) => {
      prev[`${colorKey}-${i + 1}`] = colors[i];
      prev[`${colorKey}${i + 1}`] = colors[i];
      return prev;
    }, {});
  }).reduce((prev, cur) => {
    prev = Object.assign(Object.assign({}, prev), cur);
    return prev;
  }, {});
  return Object.assign(Object.assign(Object.assign(Object.assign(Object.assign(Object.assign(Object.assign({}, token), colorPalettes), (0, _genColorMapToken.default)(token, {
    generateColorPalettes: _colors2.generateColorPalettes,
    generateNeutralColorPalettes: _colors2.generateNeutralColorPalettes
  })), (0, _genFontMapToken.default)(token.fontSize)), (0, _genSizeMapToken.default)(token)), (0, _genControlHeight.default)(token)), (0, _genCommonMapToken.default)(token));
}