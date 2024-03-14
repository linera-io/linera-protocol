"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genDraggerStyle = token => {
  const {
    componentCls,
    iconCls
  } = token;
  return {
    [`${componentCls}-wrapper`]: {
      [`${componentCls}-drag`]: {
        position: 'relative',
        width: '100%',
        height: '100%',
        textAlign: 'center',
        background: token.colorFillAlter,
        border: `${(0, _cssinjs.unit)(token.lineWidth)} dashed ${token.colorBorder}`,
        borderRadius: token.borderRadiusLG,
        cursor: 'pointer',
        transition: `border-color ${token.motionDurationSlow}`,
        [componentCls]: {
          padding: token.padding
        },
        [`${componentCls}-btn`]: {
          display: 'table',
          width: '100%',
          height: '100%',
          outline: 'none',
          borderRadius: token.borderRadiusLG,
          '&:focus-visible': {
            outline: `${(0, _cssinjs.unit)(token.lineWidthFocus)} solid ${token.colorPrimaryBorder}`
          }
        },
        [`${componentCls}-drag-container`]: {
          display: 'table-cell',
          verticalAlign: 'middle'
        },
        [`
          &:not(${componentCls}-disabled):hover,
          &-hover:not(${componentCls}-disabled)
        `]: {
          borderColor: token.colorPrimaryHover
        },
        [`p${componentCls}-drag-icon`]: {
          marginBottom: token.margin,
          [iconCls]: {
            color: token.colorPrimary,
            fontSize: token.uploadThumbnailSize
          }
        },
        [`p${componentCls}-text`]: {
          margin: `0 0 ${(0, _cssinjs.unit)(token.marginXXS)}`,
          color: token.colorTextHeading,
          fontSize: token.fontSizeLG
        },
        [`p${componentCls}-hint`]: {
          color: token.colorTextDescription,
          fontSize: token.fontSize
        },
        // ===================== Disabled =====================
        [`&${componentCls}-disabled`]: {
          [`p${componentCls}-drag-icon ${iconCls},
            p${componentCls}-text,
            p${componentCls}-hint
          `]: {
            color: token.colorTextDisabled
          }
        }
      }
    }
  };
};
var _default = exports.default = genDraggerStyle;