"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _style = require("../../style");
const genExpandStyle = token => {
  const {
    componentCls,
    antCls,
    motionDurationSlow,
    lineWidth,
    paddingXS,
    lineType,
    tableBorderColor,
    tableExpandIconBg,
    tableExpandColumnWidth,
    borderRadius,
    tablePaddingVertical,
    tablePaddingHorizontal,
    tableExpandedRowBg,
    paddingXXS,
    expandIconMarginTop,
    expandIconSize,
    expandIconHalfInner,
    expandIconScale,
    calc
  } = token;
  const tableBorder = `${(0, _cssinjs.unit)(lineWidth)} ${lineType} ${tableBorderColor}`;
  const expandIconLineOffset = calc(paddingXXS).sub(lineWidth).equal();
  return {
    [`${componentCls}-wrapper`]: {
      [`${componentCls}-expand-icon-col`]: {
        width: tableExpandColumnWidth
      },
      [`${componentCls}-row-expand-icon-cell`]: {
        textAlign: 'center',
        [`${componentCls}-row-expand-icon`]: {
          display: 'inline-flex',
          float: 'none',
          verticalAlign: 'sub'
        }
      },
      [`${componentCls}-row-indent`]: {
        height: 1,
        float: 'left'
      },
      [`${componentCls}-row-expand-icon`]: Object.assign(Object.assign({}, (0, _style.operationUnit)(token)), {
        position: 'relative',
        float: 'left',
        boxSizing: 'border-box',
        width: expandIconSize,
        height: expandIconSize,
        padding: 0,
        color: 'inherit',
        lineHeight: (0, _cssinjs.unit)(expandIconSize),
        background: tableExpandIconBg,
        border: tableBorder,
        borderRadius,
        transform: `scale(${expandIconScale})`,
        transition: `all ${motionDurationSlow}`,
        userSelect: 'none',
        [`&:focus, &:hover, &:active`]: {
          borderColor: 'currentcolor'
        },
        [`&::before, &::after`]: {
          position: 'absolute',
          background: 'currentcolor',
          transition: `transform ${motionDurationSlow} ease-out`,
          content: '""'
        },
        '&::before': {
          top: expandIconHalfInner,
          insetInlineEnd: expandIconLineOffset,
          insetInlineStart: expandIconLineOffset,
          height: lineWidth
        },
        '&::after': {
          top: expandIconLineOffset,
          bottom: expandIconLineOffset,
          insetInlineStart: expandIconHalfInner,
          width: lineWidth,
          transform: 'rotate(90deg)'
        },
        // Motion effect
        '&-collapsed::before': {
          transform: 'rotate(-180deg)'
        },
        '&-collapsed::after': {
          transform: 'rotate(0deg)'
        },
        '&-spaced': {
          '&::before, &::after': {
            display: 'none',
            content: 'none'
          },
          background: 'transparent',
          border: 0,
          visibility: 'hidden'
        }
      }),
      [`${componentCls}-row-indent + ${componentCls}-row-expand-icon`]: {
        marginTop: expandIconMarginTop,
        marginInlineEnd: paddingXS
      },
      [`tr${componentCls}-expanded-row`]: {
        '&, &:hover': {
          [`> th, > td`]: {
            background: tableExpandedRowBg
          }
        },
        // https://github.com/ant-design/ant-design/issues/25573
        [`${antCls}-descriptions-view`]: {
          display: 'flex',
          table: {
            flex: 'auto',
            width: 'auto'
          }
        }
      },
      // With fixed
      [`${componentCls}-expanded-row-fixed`]: {
        position: 'relative',
        margin: `${(0, _cssinjs.unit)(calc(tablePaddingVertical).mul(-1).equal())} ${(0, _cssinjs.unit)(calc(tablePaddingHorizontal).mul(-1).equal())}`,
        padding: `${(0, _cssinjs.unit)(tablePaddingVertical)} ${(0, _cssinjs.unit)(tablePaddingHorizontal)}`
      }
    }
  };
};
var _default = exports.default = genExpandStyle;