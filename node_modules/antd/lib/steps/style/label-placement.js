"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genStepsLabelPlacementStyle = token => {
  const {
    componentCls,
    iconSize,
    lineHeight,
    iconSizeSM
  } = token;
  return {
    [`&${componentCls}-label-vertical`]: {
      [`${componentCls}-item`]: {
        overflow: 'visible',
        '&-tail': {
          marginInlineStart: token.calc(iconSize).div(2).add(token.controlHeightLG).equal(),
          padding: `${(0, _cssinjs.unit)(token.paddingXXS)} ${(0, _cssinjs.unit)(token.paddingLG)}`
        },
        '&-content': {
          display: 'block',
          width: token.calc(iconSize).div(2).add(token.controlHeightLG).mul(2).equal(),
          marginTop: token.marginSM,
          textAlign: 'center'
        },
        '&-icon': {
          display: 'inline-block',
          marginInlineStart: token.controlHeightLG
        },
        '&-title': {
          paddingInlineEnd: 0,
          paddingInlineStart: 0,
          '&::after': {
            display: 'none'
          }
        },
        '&-subtitle': {
          display: 'block',
          marginBottom: token.marginXXS,
          marginInlineStart: 0,
          lineHeight
        }
      },
      [`&${componentCls}-small:not(${componentCls}-dot)`]: {
        [`${componentCls}-item`]: {
          '&-icon': {
            marginInlineStart: token.calc(iconSize).sub(iconSizeSM).div(2).add(token.controlHeightLG).equal()
          }
        }
      }
    }
  };
};
var _default = exports.default = genStepsLabelPlacementStyle;