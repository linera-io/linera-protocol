"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _internal = require("../../theme/internal");
// ============================== Shared ==============================
const genSharedAffixStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [componentCls]: {
      position: 'fixed',
      zIndex: token.zIndexPopup
    }
  };
};
const prepareComponentToken = token => ({
  zIndexPopup: token.zIndexBase + 10
});
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Affix', genSharedAffixStyle, prepareComponentToken);