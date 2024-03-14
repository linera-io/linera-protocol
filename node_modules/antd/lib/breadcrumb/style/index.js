"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _style = require("../../style");
var _internal = require("../../theme/internal");
const genBreadcrumbStyle = token => {
  const {
    componentCls,
    iconCls,
    calc
  } = token;
  return {
    [componentCls]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      color: token.itemColor,
      fontSize: token.fontSize,
      [iconCls]: {
        fontSize: token.iconFontSize
      },
      ol: {
        display: 'flex',
        flexWrap: 'wrap',
        margin: 0,
        padding: 0,
        listStyle: 'none'
      },
      a: Object.assign({
        color: token.linkColor,
        transition: `color ${token.motionDurationMid}`,
        padding: `0 ${(0, _cssinjs.unit)(token.paddingXXS)}`,
        borderRadius: token.borderRadiusSM,
        height: token.fontHeight,
        display: 'inline-block',
        marginInline: calc(token.marginXXS).mul(-1).equal(),
        '&:hover': {
          color: token.linkHoverColor,
          backgroundColor: token.colorBgTextHover
        }
      }, (0, _style.genFocusStyle)(token)),
      [`li:last-child`]: {
        color: token.lastItemColor
      },
      [`${componentCls}-separator`]: {
        marginInline: token.separatorMargin,
        color: token.separatorColor
      },
      [`${componentCls}-link`]: {
        [`
          > ${iconCls} + span,
          > ${iconCls} + a
        `]: {
          marginInlineStart: token.marginXXS
        }
      },
      [`${componentCls}-overlay-link`]: {
        borderRadius: token.borderRadiusSM,
        height: token.fontHeight,
        display: 'inline-block',
        padding: `0 ${(0, _cssinjs.unit)(token.paddingXXS)}`,
        marginInline: calc(token.marginXXS).mul(-1).equal(),
        [`> ${iconCls}`]: {
          marginInlineStart: token.marginXXS,
          fontSize: token.fontSizeIcon
        },
        '&:hover': {
          color: token.linkHoverColor,
          backgroundColor: token.colorBgTextHover,
          a: {
            color: token.linkHoverColor
          }
        },
        a: {
          '&:hover': {
            backgroundColor: 'transparent'
          }
        }
      },
      // rtl style
      [`&${token.componentCls}-rtl`]: {
        direction: 'rtl'
      }
    })
  };
};
const prepareComponentToken = token => ({
  itemColor: token.colorTextDescription,
  lastItemColor: token.colorText,
  iconFontSize: token.fontSize,
  linkColor: token.colorTextDescription,
  linkHoverColor: token.colorText,
  separatorColor: token.colorTextDescription,
  separatorMargin: token.marginXS
});
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Breadcrumb', token => {
  const breadcrumbToken = (0, _internal.mergeToken)(token, {});
  return genBreadcrumbStyle(breadcrumbToken);
}, prepareComponentToken);