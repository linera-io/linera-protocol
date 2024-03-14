"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _genControlHeight = _interopRequireDefault(require("../shared/genControlHeight"));
var _default2 = _interopRequireDefault(require("../default"));
var _genCompactSizeMapToken = _interopRequireDefault(require("./genCompactSizeMapToken"));
var _genFontMapToken = _interopRequireDefault(require("../shared/genFontMapToken"));
const derivative = (token, mapToken) => {
  const mergedMapToken = mapToken !== null && mapToken !== void 0 ? mapToken : (0, _default2.default)(token);
  const fontSize = mergedMapToken.fontSizeSM; // Smaller size font-size as base
  const controlHeight = mergedMapToken.controlHeight - 4;
  return Object.assign(Object.assign(Object.assign(Object.assign(Object.assign({}, mergedMapToken), (0, _genCompactSizeMapToken.default)(mapToken !== null && mapToken !== void 0 ? mapToken : token)), (0, _genFontMapToken.default)(fontSize)), {
    // controlHeight
    controlHeight
  }), (0, _genControlHeight.default)(Object.assign(Object.assign({}, mergedMapToken), {
    controlHeight
  })));
};
var _default = exports.default = derivative;