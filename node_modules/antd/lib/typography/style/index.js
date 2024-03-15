"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _style = require("../../style");
var _internal = require("../../theme/internal");
var _mixins = require("./mixins");
const genTypographyStyle = token => {
  const {
    componentCls,
    titleMarginTop
  } = token;
  return {
    [componentCls]: Object.assign(Object.assign(Object.assign(Object.assign(Object.assign(Object.assign(Object.assign(Object.assign(Object.assign({
      color: token.colorText,
      wordBreak: 'break-word',
      lineHeight: token.lineHeight,
      [`&${componentCls}-secondary`]: {
        color: token.colorTextDescription
      },
      [`&${componentCls}-success`]: {
        color: token.colorSuccess
      },
      [`&${componentCls}-warning`]: {
        color: token.colorWarning
      },
      [`&${componentCls}-danger`]: {
        color: token.colorError,
        'a&:active, a&:focus': {
          color: token.colorErrorActive
        },
        'a&:hover': {
          color: token.colorErrorHover
        }
      },
      [`&${componentCls}-disabled`]: {
        color: token.colorTextDisabled,
        cursor: 'not-allowed',
        userSelect: 'none'
      },
      [`
        div&,
        p
      `]: {
        marginBottom: '1em'
      }
    }, (0, _mixins.getTitleStyles)(token)), {
      [`
      & + h1${componentCls},
      & + h2${componentCls},
      & + h3${componentCls},
      & + h4${componentCls},
      & + h5${componentCls}
      `]: {
        marginTop: titleMarginTop
      },
      [`
      div,
      ul,
      li,
      p,
      h1,
      h2,
      h3,
      h4,
      h5`]: {
        [`
        + h1,
        + h2,
        + h3,
        + h4,
        + h5
        `]: {
          marginTop: titleMarginTop
        }
      }
    }), (0, _mixins.getResetStyles)(token)), (0, _mixins.getLinkStyles)(token)), {
      // Operation
      [`
        ${componentCls}-expand,
        ${componentCls}-edit,
        ${componentCls}-copy
      `]: Object.assign(Object.assign({}, (0, _style.operationUnit)(token)), {
        marginInlineStart: token.marginXXS
      })
    }), (0, _mixins.getEditableStyles)(token)), (0, _mixins.getCopyableStyles)(token)), (0, _mixins.getEllipsisStyles)()), {
      '&-rtl': {
        direction: 'rtl'
      }
    })
  };
};
const prepareComponentToken = () => ({
  titleMarginTop: '1.2em',
  titleMarginBottom: '0.5em'
});
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Typography', token => [genTypographyStyle(token)], prepareComponentToken);