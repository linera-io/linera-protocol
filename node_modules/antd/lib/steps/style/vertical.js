"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genStepsVerticalStyle = token => {
  const {
    componentCls,
    iconSizeSM,
    iconSize
  } = token;
  return {
    [`&${componentCls}-vertical`]: {
      display: 'flex',
      flexDirection: 'column',
      [`> ${componentCls}-item`]: {
        display: 'block',
        flex: '1 0 auto',
        paddingInlineStart: 0,
        overflow: 'visible',
        [`${componentCls}-item-icon`]: {
          float: 'left',
          marginInlineEnd: token.margin
        },
        [`${componentCls}-item-content`]: {
          display: 'block',
          minHeight: token.calc(token.controlHeight).mul(1.5).equal(),
          overflow: 'hidden'
        },
        [`${componentCls}-item-title`]: {
          lineHeight: `${(0, _cssinjs.unit)(iconSize)}`
        },
        [`${componentCls}-item-description`]: {
          paddingBottom: token.paddingSM
        }
      },
      [`> ${componentCls}-item > ${componentCls}-item-container > ${componentCls}-item-tail`]: {
        position: 'absolute',
        top: 0,
        insetInlineStart: token.calc(iconSize).div(2).sub(token.lineWidth).equal(),
        width: token.lineWidth,
        height: '100%',
        padding: `${(0, _cssinjs.unit)(token.calc(token.marginXXS).mul(1.5).add(iconSize).equal())} 0 ${(0, _cssinjs.unit)(token.calc(token.marginXXS).mul(1.5).equal())}`,
        '&::after': {
          width: token.lineWidth,
          height: '100%'
        }
      },
      [`> ${componentCls}-item:not(:last-child) > ${componentCls}-item-container > ${componentCls}-item-tail`]: {
        display: 'block'
      },
      [` > ${componentCls}-item > ${componentCls}-item-container > ${componentCls}-item-content > ${componentCls}-item-title`]: {
        '&::after': {
          display: 'none'
        }
      },
      [`&${componentCls}-small ${componentCls}-item-container`]: {
        [`${componentCls}-item-tail`]: {
          position: 'absolute',
          top: 0,
          insetInlineStart: token.calc(iconSizeSM).div(2).sub(token.lineWidth).equal(),
          padding: `${(0, _cssinjs.unit)(token.calc(token.marginXXS).mul(1.5).add(iconSizeSM).equal())} 0 ${(0, _cssinjs.unit)(token.calc(token.marginXXS).mul(1.5).equal())}`
        },
        [`${componentCls}-item-title`]: {
          lineHeight: `${(0, _cssinjs.unit)(iconSizeSM)}`
        }
      }
    }
  };
};
var _default = exports.default = genStepsVerticalStyle;