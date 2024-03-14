import { unit } from '@ant-design/cssinjs';
import { resetComponent, textEllipsis } from '../../style';
import { genStyleHooks, mergeToken } from '../../theme/internal';
const genBorderedStyle = token => {
  const {
    componentCls,
    labelBg
  } = token;
  return {
    [`&${componentCls}-bordered`]: {
      [`> ${componentCls}-view`]: {
        border: `${unit(token.lineWidth)} ${token.lineType} ${token.colorSplit}`,
        '> table': {
          tableLayout: 'auto'
        },
        [`${componentCls}-row`]: {
          borderBottom: `${unit(token.lineWidth)} ${token.lineType} ${token.colorSplit}`,
          '&:last-child': {
            borderBottom: 'none'
          },
          [`> ${componentCls}-item-label, > ${componentCls}-item-content`]: {
            padding: `${unit(token.padding)} ${unit(token.paddingLG)}`,
            borderInlineEnd: `${unit(token.lineWidth)} ${token.lineType} ${token.colorSplit}`,
            '&:last-child': {
              borderInlineEnd: 'none'
            }
          },
          [`> ${componentCls}-item-label`]: {
            color: token.colorTextSecondary,
            backgroundColor: labelBg,
            '&::after': {
              display: 'none'
            }
          }
        }
      },
      [`&${componentCls}-middle`]: {
        [`${componentCls}-row`]: {
          [`> ${componentCls}-item-label, > ${componentCls}-item-content`]: {
            padding: `${unit(token.paddingSM)} ${unit(token.paddingLG)}`
          }
        }
      },
      [`&${componentCls}-small`]: {
        [`${componentCls}-row`]: {
          [`> ${componentCls}-item-label, > ${componentCls}-item-content`]: {
            padding: `${unit(token.paddingXS)} ${unit(token.padding)}`
          }
        }
      }
    }
  };
};
const genDescriptionStyles = token => {
  const {
    componentCls,
    extraColor,
    itemPaddingBottom,
    colonMarginRight,
    colonMarginLeft,
    titleMarginBottom
  } = token;
  return {
    [componentCls]: Object.assign(Object.assign(Object.assign({}, resetComponent(token)), genBorderedStyle(token)), {
      [`&-rtl`]: {
        direction: 'rtl'
      },
      [`${componentCls}-header`]: {
        display: 'flex',
        alignItems: 'center',
        marginBottom: titleMarginBottom
      },
      [`${componentCls}-title`]: Object.assign(Object.assign({}, textEllipsis), {
        flex: 'auto',
        color: token.titleColor,
        fontWeight: token.fontWeightStrong,
        fontSize: token.fontSizeLG,
        lineHeight: token.lineHeightLG
      }),
      [`${componentCls}-extra`]: {
        marginInlineStart: 'auto',
        color: extraColor,
        fontSize: token.fontSize
      },
      [`${componentCls}-view`]: {
        width: '100%',
        borderRadius: token.borderRadiusLG,
        table: {
          width: '100%',
          tableLayout: 'fixed',
          borderCollapse: 'collapse'
        }
      },
      [`${componentCls}-row`]: {
        '> th, > td': {
          paddingBottom: itemPaddingBottom
        },
        '&:last-child': {
          borderBottom: 'none'
        }
      },
      [`${componentCls}-item-label`]: {
        color: token.colorTextTertiary,
        fontWeight: 'normal',
        fontSize: token.fontSize,
        lineHeight: token.lineHeight,
        textAlign: 'start',
        '&::after': {
          content: '":"',
          position: 'relative',
          top: -0.5,
          // magic for position
          marginInline: `${unit(colonMarginLeft)} ${unit(colonMarginRight)}`
        },
        [`&${componentCls}-item-no-colon::after`]: {
          content: '""'
        }
      },
      [`${componentCls}-item-no-label`]: {
        '&::after': {
          margin: 0,
          content: '""'
        }
      },
      [`${componentCls}-item-content`]: {
        display: 'table-cell',
        flex: 1,
        color: token.contentColor,
        fontSize: token.fontSize,
        lineHeight: token.lineHeight,
        wordBreak: 'break-word',
        overflowWrap: 'break-word'
      },
      [`${componentCls}-item`]: {
        paddingBottom: 0,
        verticalAlign: 'top',
        '&-container': {
          display: 'flex',
          [`${componentCls}-item-label`]: {
            display: 'inline-flex',
            alignItems: 'baseline'
          },
          [`${componentCls}-item-content`]: {
            display: 'inline-flex',
            alignItems: 'baseline'
          }
        }
      },
      '&-middle': {
        [`${componentCls}-row`]: {
          '> th, > td': {
            paddingBottom: token.paddingSM
          }
        }
      },
      '&-small': {
        [`${componentCls}-row`]: {
          '> th, > td': {
            paddingBottom: token.paddingXS
          }
        }
      }
    })
  };
};
export const prepareComponentToken = token => ({
  labelBg: token.colorFillAlter,
  titleColor: token.colorText,
  titleMarginBottom: token.fontSizeSM * token.lineHeightSM,
  itemPaddingBottom: token.padding,
  colonMarginRight: token.marginXS,
  colonMarginLeft: token.marginXXS / 2,
  contentColor: token.colorText,
  extraColor: token.colorText
});
// ============================== Export ==============================
export default genStyleHooks('Descriptions', token => {
  const descriptionToken = mergeToken(token, {});
  return genDescriptionStyles(descriptionToken);
}, prepareComponentToken);