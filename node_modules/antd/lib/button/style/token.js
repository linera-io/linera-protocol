"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareToken = exports.prepareComponentToken = void 0;
var _internal = require("../../theme/internal");
const prepareToken = token => {
  const {
    paddingInline,
    onlyIconSize,
    paddingBlock
  } = token;
  const buttonToken = (0, _internal.mergeToken)(token, {
    buttonPaddingHorizontal: paddingInline,
    buttonPaddingVertical: paddingBlock,
    buttonIconOnlyFontSize: onlyIconSize
  });
  return buttonToken;
};
exports.prepareToken = prepareToken;
const prepareComponentToken = token => {
  var _a, _b, _c, _d, _e, _f;
  const contentFontSize = (_a = token.contentFontSize) !== null && _a !== void 0 ? _a : token.fontSize;
  const contentFontSizeSM = (_b = token.contentFontSizeSM) !== null && _b !== void 0 ? _b : token.fontSize;
  const contentFontSizeLG = (_c = token.contentFontSizeLG) !== null && _c !== void 0 ? _c : token.fontSizeLG;
  const contentLineHeight = (_d = token.contentLineHeight) !== null && _d !== void 0 ? _d : (0, _internal.getLineHeight)(contentFontSize);
  const contentLineHeightSM = (_e = token.contentLineHeightSM) !== null && _e !== void 0 ? _e : (0, _internal.getLineHeight)(contentFontSizeSM);
  const contentLineHeightLG = (_f = token.contentLineHeightLG) !== null && _f !== void 0 ? _f : (0, _internal.getLineHeight)(contentFontSizeLG);
  return {
    fontWeight: 400,
    defaultShadow: `0 ${token.controlOutlineWidth}px 0 ${token.controlTmpOutline}`,
    primaryShadow: `0 ${token.controlOutlineWidth}px 0 ${token.controlOutline}`,
    dangerShadow: `0 ${token.controlOutlineWidth}px 0 ${token.colorErrorOutline}`,
    primaryColor: token.colorTextLightSolid,
    dangerColor: token.colorTextLightSolid,
    borderColorDisabled: token.colorBorder,
    defaultGhostColor: token.colorBgContainer,
    ghostBg: 'transparent',
    defaultGhostBorderColor: token.colorBgContainer,
    paddingInline: token.paddingContentHorizontal - token.lineWidth,
    paddingInlineLG: token.paddingContentHorizontal - token.lineWidth,
    paddingInlineSM: 8 - token.lineWidth,
    onlyIconSize: token.fontSizeLG,
    onlyIconSizeSM: token.fontSizeLG - 2,
    onlyIconSizeLG: token.fontSizeLG + 2,
    groupBorderColor: token.colorPrimaryHover,
    linkHoverBg: 'transparent',
    textHoverBg: token.colorBgTextHover,
    defaultColor: token.colorText,
    defaultBg: token.colorBgContainer,
    defaultBorderColor: token.colorBorder,
    defaultBorderColorDisabled: token.colorBorder,
    defaultHoverBg: token.colorBgContainer,
    defaultHoverColor: token.colorPrimaryHover,
    defaultHoverBorderColor: token.colorPrimaryHover,
    defaultActiveBg: token.colorBgContainer,
    defaultActiveColor: token.colorPrimaryActive,
    defaultActiveBorderColor: token.colorPrimaryActive,
    contentFontSize,
    contentFontSizeSM,
    contentFontSizeLG,
    contentLineHeight,
    contentLineHeightSM,
    contentLineHeightLG,
    paddingBlock: Math.max((token.controlHeight - contentFontSize * contentLineHeight) / 2 - token.lineWidth, 0),
    paddingBlockSM: Math.max((token.controlHeightSM - contentFontSizeSM * contentLineHeightSM) / 2 - token.lineWidth, 0),
    paddingBlockLG: Math.max((token.controlHeightLG - contentFontSizeLG * contentLineHeightLG) / 2 - token.lineWidth, 0)
  };
};
exports.prepareComponentToken = prepareComponentToken;