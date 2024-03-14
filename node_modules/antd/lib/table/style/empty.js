"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
// ========================= Placeholder ==========================
const genEmptyStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [`${componentCls}-wrapper`]: {
      [`${componentCls}-tbody > tr${componentCls}-placeholder`]: {
        textAlign: 'center',
        color: token.colorTextDisabled,
        [`
          &:hover > th,
          &:hover > td,
        `]: {
          background: token.colorBgContainer
        }
      }
    }
  };
};
var _default = exports.default = genEmptyStyle;