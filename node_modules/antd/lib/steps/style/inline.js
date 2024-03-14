"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genStepsInlineStyle = token => {
  const {
    componentCls,
    inlineDotSize,
    inlineTitleColor,
    inlineTailColor
  } = token;
  const containerPaddingTop = token.calc(token.paddingXS).add(token.lineWidth).equal();
  const titleStyle = {
    [`${componentCls}-item-container ${componentCls}-item-content ${componentCls}-item-title`]: {
      color: inlineTitleColor
    }
  };
  return {
    [`&${componentCls}-inline`]: {
      width: 'auto',
      display: 'inline-flex',
      [`${componentCls}-item`]: {
        flex: 'none',
        '&-container': {
          padding: `${(0, _cssinjs.unit)(containerPaddingTop)} ${(0, _cssinjs.unit)(token.paddingXXS)} 0`,
          margin: `0 ${(0, _cssinjs.unit)(token.calc(token.marginXXS).div(2).equal())}`,
          borderRadius: token.borderRadiusSM,
          cursor: 'pointer',
          transition: `background-color ${token.motionDurationMid}`,
          '&:hover': {
            background: token.controlItemBgHover
          },
          [`&[role='button']:hover`]: {
            opacity: 1
          }
        },
        '&-icon': {
          width: inlineDotSize,
          height: inlineDotSize,
          marginInlineStart: `calc(50% - ${(0, _cssinjs.unit)(token.calc(inlineDotSize).div(2).equal())})`,
          [`> ${componentCls}-icon`]: {
            top: 0
          },
          [`${componentCls}-icon-dot`]: {
            borderRadius: token.calc(token.fontSizeSM).div(4).equal(),
            '&::after': {
              display: 'none'
            }
          }
        },
        '&-content': {
          width: 'auto',
          marginTop: token.calc(token.marginXS).sub(token.lineWidth).equal()
        },
        '&-title': {
          color: inlineTitleColor,
          fontSize: token.fontSizeSM,
          lineHeight: token.lineHeightSM,
          fontWeight: 'normal',
          marginBottom: token.calc(token.marginXXS).div(2).equal()
        },
        '&-description': {
          display: 'none'
        },
        '&-tail': {
          marginInlineStart: 0,
          top: token.calc(inlineDotSize).div(2).add(containerPaddingTop).equal(),
          transform: `translateY(-50%)`,
          '&:after': {
            width: '100%',
            height: token.lineWidth,
            borderRadius: 0,
            marginInlineStart: 0,
            background: inlineTailColor
          }
        },
        [`&:first-child ${componentCls}-item-tail`]: {
          width: '50%',
          marginInlineStart: '50%'
        },
        [`&:last-child ${componentCls}-item-tail`]: {
          display: 'block',
          width: '50%'
        },
        '&-wait': Object.assign({
          [`${componentCls}-item-icon ${componentCls}-icon ${componentCls}-icon-dot`]: {
            backgroundColor: token.colorBorderBg,
            border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${inlineTailColor}`
          }
        }, titleStyle),
        '&-finish': Object.assign({
          [`${componentCls}-item-tail::after`]: {
            backgroundColor: inlineTailColor
          },
          [`${componentCls}-item-icon ${componentCls}-icon ${componentCls}-icon-dot`]: {
            backgroundColor: inlineTailColor,
            border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${inlineTailColor}`
          }
        }, titleStyle),
        '&-error': titleStyle,
        '&-active, &-process': Object.assign({
          [`${componentCls}-item-icon`]: {
            width: inlineDotSize,
            height: inlineDotSize,
            marginInlineStart: `calc(50% - ${(0, _cssinjs.unit)(token.calc(inlineDotSize).div(2).equal())})`,
            top: 0
          }
        }, titleStyle),
        [`&:not(${componentCls}-item-active) > ${componentCls}-item-container[role='button']:hover`]: {
          [`${componentCls}-item-title`]: {
            color: inlineTitleColor
          }
        }
      }
    }
  };
};
var _default = exports.default = genStepsInlineStyle;