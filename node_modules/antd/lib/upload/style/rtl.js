"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
// =========================== Motion ===========================
const genRtlStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [`${componentCls}-rtl`]: {
      direction: 'rtl'
    }
  };
};
var _default = exports.default = genRtlStyle;