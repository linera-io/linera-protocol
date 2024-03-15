"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genStepsSmallStyle = token => {
  const {
    componentCls,
    iconSizeSM,
    // stepsSmallIconMargin,
    fontSizeSM,
    fontSize,
    colorTextDescription
  } = token;
  return {
    [`&${componentCls}-small`]: {
      [`&${componentCls}-horizontal:not(${componentCls}-label-vertical) ${componentCls}-item`]: {
        paddingInlineStart: token.paddingSM,
        '&:first-child': {
          paddingInlineStart: 0
        }
      },
      [`${componentCls}-item-icon`]: {
        width: iconSizeSM,
        height: iconSizeSM,
        // margin: stepsSmallIconMargin,
        marginTop: 0,
        marginBottom: 0,
        marginInline: `0 ${(0, _cssinjs.unit)(token.marginXS)}`,
        fontSize: fontSizeSM,
        lineHeight: `${(0, _cssinjs.unit)(iconSizeSM)}`,
        textAlign: 'center',
        borderRadius: iconSizeSM
      },
      [`${componentCls}-item-title`]: {
        paddingInlineEnd: token.paddingSM,
        fontSize,
        lineHeight: `${(0, _cssinjs.unit)(iconSizeSM)}`,
        '&::after': {
          top: token.calc(iconSizeSM).div(2).equal()
        }
      },
      [`${componentCls}-item-description`]: {
        color: colorTextDescription,
        fontSize
      },
      [`${componentCls}-item-tail`]: {
        top: token.calc(iconSizeSM).div(2).sub(token.paddingXXS).equal()
      },
      [`${componentCls}-item-custom ${componentCls}-item-icon`]: {
        width: 'inherit',
        height: 'inherit',
        lineHeight: 'inherit',
        background: 'none',
        border: 0,
        borderRadius: 0,
        [`> ${componentCls}-icon`]: {
          fontSize: iconSizeSM,
          lineHeight: `${(0, _cssinjs.unit)(iconSizeSM)}`,
          transform: 'none'
        }
      }
    }
  };
};
var _default = exports.default = genStepsSmallStyle;