"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ = require(".");
var _internal = require("../../theme/internal");
/**
 * Fallback of IE.
 * Safe to remove.
 */
// Style as inline component

// ============================= Fallback =============================
const genFallbackStyle = token => {
  const {
    formItemCls
  } = token;
  return {
    '@media screen and (-ms-high-contrast: active), (-ms-high-contrast: none)': {
      // Fallback for IE, safe to remove we not support it anymore
      [`${formItemCls}-control`]: {
        display: 'flex'
      }
    }
  };
};
// ============================== Export ==============================
var _default = exports.default = (0, _internal.genSubStyleComponent)(['Form', 'item-item'], (token, _ref) => {
  let {
    rootPrefixCls
  } = _ref;
  const formToken = (0, _.prepareToken)(token, rootPrefixCls);
  return [genFallbackStyle(formToken)];
});