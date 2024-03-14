"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _genComponentStyleHook = require("../../theme/util/genComponentStyleHook");
var _ = require(".");
var _cssinjs = require("@ant-design/cssinjs");
const genBorderedStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [`${componentCls}${componentCls}-bordered${componentCls}-disabled:not(${componentCls}-mini)`]: {
      '&, &:hover': {
        [`${componentCls}-item-link`]: {
          borderColor: token.colorBorder
        }
      },
      '&:focus-visible': {
        [`${componentCls}-item-link`]: {
          borderColor: token.colorBorder
        }
      },
      [`${componentCls}-item, ${componentCls}-item-link`]: {
        backgroundColor: token.colorBgContainerDisabled,
        borderColor: token.colorBorder,
        [`&:hover:not(${componentCls}-item-active)`]: {
          backgroundColor: token.colorBgContainerDisabled,
          borderColor: token.colorBorder,
          a: {
            color: token.colorTextDisabled
          }
        },
        [`&${componentCls}-item-active`]: {
          backgroundColor: token.itemActiveBgDisabled
        }
      },
      [`${componentCls}-prev, ${componentCls}-next`]: {
        '&:hover button': {
          backgroundColor: token.colorBgContainerDisabled,
          borderColor: token.colorBorder,
          color: token.colorTextDisabled
        },
        [`${componentCls}-item-link`]: {
          backgroundColor: token.colorBgContainerDisabled,
          borderColor: token.colorBorder
        }
      }
    },
    [`${componentCls}${componentCls}-bordered:not(${componentCls}-mini)`]: {
      [`${componentCls}-prev, ${componentCls}-next`]: {
        '&:hover button': {
          borderColor: token.colorPrimaryHover,
          backgroundColor: token.itemBg
        },
        [`${componentCls}-item-link`]: {
          backgroundColor: token.itemLinkBg,
          borderColor: token.colorBorder
        },
        [`&:hover ${componentCls}-item-link`]: {
          borderColor: token.colorPrimary,
          backgroundColor: token.itemBg,
          color: token.colorPrimary
        },
        [`&${componentCls}-disabled`]: {
          [`${componentCls}-item-link`]: {
            borderColor: token.colorBorder,
            color: token.colorTextDisabled
          }
        }
      },
      [`${componentCls}-item`]: {
        backgroundColor: token.itemBg,
        border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.colorBorder}`,
        [`&:hover:not(${componentCls}-item-active)`]: {
          borderColor: token.colorPrimary,
          backgroundColor: token.itemBg,
          a: {
            color: token.colorPrimary
          }
        },
        '&-active': {
          borderColor: token.colorPrimary
        }
      }
    }
  };
};
var _default = exports.default = (0, _genComponentStyleHook.genSubStyleComponent)(['Pagination', 'bordered'], token => {
  const paginationToken = (0, _.prepareToken)(token);
  return [genBorderedStyle(paginationToken)];
}, _.prepareComponentToken);