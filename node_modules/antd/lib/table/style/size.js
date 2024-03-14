"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genSizeStyle = token => {
  const {
    componentCls,
    tableExpandColumnWidth,
    calc
  } = token;
  const getSizeStyle = (size, paddingVertical, paddingHorizontal, fontSize) => ({
    [`${componentCls}${componentCls}-${size}`]: {
      fontSize,
      [`
        ${componentCls}-title,
        ${componentCls}-footer,
        ${componentCls}-cell,
        ${componentCls}-thead > tr > th,
        ${componentCls}-tbody > tr > th,
        ${componentCls}-tbody > tr > td,
        tfoot > tr > th,
        tfoot > tr > td
      `]: {
        padding: `${(0, _cssinjs.unit)(paddingVertical)} ${(0, _cssinjs.unit)(paddingHorizontal)}`
      },
      [`${componentCls}-filter-trigger`]: {
        marginInlineEnd: (0, _cssinjs.unit)(calc(paddingHorizontal).div(2).mul(-1).equal())
      },
      [`${componentCls}-expanded-row-fixed`]: {
        margin: `${(0, _cssinjs.unit)(calc(paddingVertical).mul(-1).equal())} ${(0, _cssinjs.unit)(calc(paddingHorizontal).mul(-1).equal())}`
      },
      [`${componentCls}-tbody`]: {
        // ========================= Nest Table ===========================
        [`${componentCls}-wrapper:only-child ${componentCls}`]: {
          marginBlock: (0, _cssinjs.unit)(calc(paddingVertical).mul(-1).equal()),
          marginInline: `${(0, _cssinjs.unit)(calc(tableExpandColumnWidth).sub(paddingHorizontal).equal())} ${(0, _cssinjs.unit)(calc(paddingHorizontal).mul(-1).equal())}`
        }
      },
      // https://github.com/ant-design/ant-design/issues/35167
      [`${componentCls}-selection-extra`]: {
        paddingInlineStart: (0, _cssinjs.unit)(calc(paddingHorizontal).div(4).equal())
      }
    }
  });
  return {
    [`${componentCls}-wrapper`]: Object.assign(Object.assign({}, getSizeStyle('middle', token.tablePaddingVerticalMiddle, token.tablePaddingHorizontalMiddle, token.tableFontSizeMiddle)), getSizeStyle('small', token.tablePaddingVerticalSmall, token.tablePaddingHorizontalSmall, token.tableFontSizeSmall))
  };
};
var _default = exports.default = genSizeStyle;