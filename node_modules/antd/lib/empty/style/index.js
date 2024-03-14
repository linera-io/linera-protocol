"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _internal = require("../../theme/internal");
// ============================== Shared ==============================
const genSharedEmptyStyle = token => {
  const {
    componentCls,
    margin,
    marginXS,
    marginXL,
    fontSize,
    lineHeight
  } = token;
  return {
    [componentCls]: {
      marginInline: marginXS,
      fontSize,
      lineHeight,
      textAlign: 'center',
      // 原来 &-image 没有父子结构，现在为了外层承担我们的hashId，改成父子结果
      [`${componentCls}-image`]: {
        height: token.emptyImgHeight,
        marginBottom: marginXS,
        opacity: token.opacityImage,
        img: {
          height: '100%'
        },
        svg: {
          maxWidth: '100%',
          height: '100%',
          margin: 'auto'
        }
      },
      [`${componentCls}-description`]: {
        color: token.colorText
      },
      // 原来 &-footer 没有父子结构，现在为了外层承担我们的hashId，改成父子结果
      [`${componentCls}-footer`]: {
        marginTop: margin
      },
      '&-normal': {
        marginBlock: marginXL,
        color: token.colorTextDisabled,
        [`${componentCls}-description`]: {
          color: token.colorTextDisabled
        },
        [`${componentCls}-image`]: {
          height: token.emptyImgHeightMD
        }
      },
      '&-small': {
        marginBlock: marginXS,
        color: token.colorTextDisabled,
        [`${componentCls}-image`]: {
          height: token.emptyImgHeightSM
        }
      }
    }
  };
};
// ============================== Export ==============================
var _default = exports.default = (0, _internal.genStyleHooks)('Empty', token => {
  const {
    componentCls,
    controlHeightLG,
    calc
  } = token;
  const emptyToken = (0, _internal.mergeToken)(token, {
    emptyImgCls: `${componentCls}-img`,
    emptyImgHeight: calc(controlHeightLG).mul(2.5).equal(),
    emptyImgHeightMD: controlHeightLG,
    emptyImgHeightSM: calc(controlHeightLG).mul(0.875).equal()
  });
  return [genSharedEmptyStyle(emptyToken)];
});