"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const getRTLStyle = _ref => {
  let {
    componentCls,
    menuArrowOffset,
    calc
  } = _ref;
  return {
    [`${componentCls}-rtl`]: {
      direction: 'rtl'
    },
    [`${componentCls}-submenu-rtl`]: {
      transformOrigin: '100% 0'
    },
    // Vertical Arrow
    [`${componentCls}-rtl${componentCls}-vertical,
    ${componentCls}-submenu-rtl ${componentCls}-vertical`]: {
      [`${componentCls}-submenu-arrow`]: {
        '&::before': {
          transform: `rotate(-45deg) translateY(${(0, _cssinjs.unit)(calc(menuArrowOffset).mul(-1).equal())})`
        },
        '&::after': {
          transform: `rotate(45deg) translateY(${(0, _cssinjs.unit)(menuArrowOffset)})`
        }
      }
    }
  };
};
var _default = exports.default = getRTLStyle;