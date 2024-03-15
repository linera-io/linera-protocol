"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _variants = require("../../input/style/variants");
const genVariantsStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [componentCls]: [Object.assign(Object.assign(Object.assign({}, (0, _variants.genOutlinedStyle)(token)), (0, _variants.genFilledStyle)(token)), (0, _variants.genBorderlessStyle)(token)),
    // ========================= Multiple =========================
    {
      '&-outlined': {
        [`&${componentCls}-multiple ${componentCls}-selection-item`]: {
          background: token.multipleItemBg,
          border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.multipleItemBorderColor}`
        }
      },
      '&-filled': {
        [`&${componentCls}-multiple ${componentCls}-selection-item`]: {
          background: token.colorBgContainer,
          border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.colorSplit}`
        }
      },
      '&-borderless': {
        [`&${componentCls}-multiple ${componentCls}-selection-item`]: {
          background: token.multipleItemBg,
          border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.multipleItemBorderColor}`
        }
      }
    }]
  };
};
var _default = exports.default = genVariantsStyle;