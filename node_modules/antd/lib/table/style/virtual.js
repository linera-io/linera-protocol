"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
const genVirtualStyle = token => {
  const {
    componentCls,
    motionDurationMid,
    lineWidth,
    lineType,
    tableBorderColor,
    calc
  } = token;
  const tableBorder = `${(0, _cssinjs.unit)(lineWidth)} ${lineType} ${tableBorderColor}`;
  const rowCellCls = `${componentCls}-expanded-row-cell`;
  return {
    [`${componentCls}-wrapper`]: {
      // ========================== Row ==========================
      [`${componentCls}-tbody-virtual`]: {
        [`${componentCls}-row:not(tr)`]: {
          display: 'flex',
          boxSizing: 'border-box',
          width: '100%'
        },
        [`${componentCls}-cell`]: {
          borderBottom: tableBorder,
          transition: `background ${motionDurationMid}`
        },
        [`${componentCls}-expanded-row`]: {
          [`${rowCellCls}${rowCellCls}-fixed`]: {
            position: 'sticky',
            insetInlineStart: 0,
            overflow: 'hidden',
            width: `calc(var(--virtual-width) - ${(0, _cssinjs.unit)(lineWidth)})`,
            borderInlineEnd: 'none'
          }
        }
      },
      // ======================== Border =========================
      [`${componentCls}-bordered`]: {
        [`${componentCls}-tbody-virtual`]: {
          '&:after': {
            content: '""',
            insetInline: 0,
            bottom: 0,
            borderBottom: tableBorder,
            position: 'absolute'
          },
          [`${componentCls}-cell`]: {
            borderInlineEnd: tableBorder,
            [`&${componentCls}-cell-fix-right-first:before`]: {
              content: '""',
              position: 'absolute',
              insetBlock: 0,
              insetInlineStart: calc(lineWidth).mul(-1).equal(),
              borderInlineStart: tableBorder
            }
          }
        },
        // Empty placeholder
        [`&${componentCls}-virtual`]: {
          [`${componentCls}-placeholder ${componentCls}-cell`]: {
            borderInlineEnd: tableBorder,
            borderBottom: tableBorder
          }
        }
      }
    }
  };
};
var _default = exports.default = genVirtualStyle;