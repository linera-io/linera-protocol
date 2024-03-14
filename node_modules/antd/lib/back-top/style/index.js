"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _style = require("../../style");
var _internal = require("../../theme/internal");
// ============================== Shared ==============================
const genSharedBackTopStyle = token => {
  const {
    componentCls,
    backTopFontSize,
    backTopSize,
    zIndexPopup
  } = token;
  return {
    [componentCls]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      position: 'fixed',
      insetInlineEnd: token.backTopInlineEnd,
      insetBlockEnd: token.backTopBlockEnd,
      zIndex: zIndexPopup,
      width: 40,
      height: 40,
      cursor: 'pointer',
      '&:empty': {
        display: 'none'
      },
      [`${componentCls}-content`]: {
        width: backTopSize,
        height: backTopSize,
        overflow: 'hidden',
        color: token.backTopColor,
        textAlign: 'center',
        backgroundColor: token.backTopBackground,
        borderRadius: backTopSize,
        transition: `all ${token.motionDurationMid}`,
        '&:hover': {
          backgroundColor: token.backTopHoverBackground,
          transition: `all ${token.motionDurationMid}`
        }
      },
      // change to .backtop .backtop-icon
      [`${componentCls}-icon`]: {
        fontSize: backTopFontSize,
        lineHeight: (0, _cssinjs.unit)(backTopSize)
      }
    })
  };
};
const genMediaBackTopStyle = token => {
  const {
    componentCls,
    screenMD,
    screenXS,
    backTopInlineEndMD,
    backTopInlineEndXS
  } = token;
  return {
    [`@media (max-width: ${(0, _cssinjs.unit)(screenMD)})`]: {
      [componentCls]: {
        insetInlineEnd: backTopInlineEndMD
      }
    },
    [`@media (max-width: ${(0, _cssinjs.unit)(screenXS)})`]: {
      [componentCls]: {
        insetInlineEnd: backTopInlineEndXS
      }
    }
  };
};
const prepareComponentToken = token => ({
  zIndexPopup: token.zIndexBase + 10
});
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('BackTop', token => {
  const {
    fontSizeHeading3,
    colorTextDescription,
    colorTextLightSolid,
    colorText,
    controlHeightLG,
    calc
  } = token;
  const backTopToken = (0, _internal.mergeToken)(token, {
    backTopBackground: colorTextDescription,
    backTopColor: colorTextLightSolid,
    backTopHoverBackground: colorText,
    backTopFontSize: fontSizeHeading3,
    backTopSize: controlHeightLG,
    backTopBlockEnd: calc(controlHeightLG).mul(1.25).equal(),
    backTopInlineEnd: calc(controlHeightLG).mul(2.5).equal(),
    backTopInlineEndMD: calc(controlHeightLG).mul(1.5).equal(),
    backTopInlineEndXS: calc(controlHeightLG).mul(0.5).equal()
  });
  return [genSharedBackTopStyle(backTopToken), genMediaBackTopStyle(backTopToken)];
}, prepareComponentToken);