import { unit } from '@ant-design/cssinjs';
import { TinyColor } from '@ctrl/tinycolor';
import { clearFix, resetComponent } from '../../style';
import { genStyleHooks, mergeToken } from '../../theme/internal';
import genBorderedStyle from './bordered';
import genEllipsisStyle from './ellipsis';
import genEmptyStyle from './empty';
import genExpandStyle from './expand';
import genFilterStyle from './filter';
import genFixedStyle from './fixed';
import genPaginationStyle from './pagination';
import genRadiusStyle from './radius';
import genRtlStyle from './rtl';
import genSelectionStyle from './selection';
import genSizeStyle from './size';
import genSorterStyle from './sorter';
import genStickyStyle from './sticky';
import genSummaryStyle from './summary';
import genVirtualStyle from './virtual';
const genTableStyle = token => {
  const {
    componentCls,
    fontWeightStrong,
    tablePaddingVertical,
    tablePaddingHorizontal,
    tableExpandColumnWidth,
    lineWidth,
    lineType,
    tableBorderColor,
    tableFontSize,
    tableBg,
    tableRadius,
    tableHeaderTextColor,
    motionDurationMid,
    tableHeaderBg,
    tableHeaderCellSplitColor,
    tableFooterTextColor,
    tableFooterBg,
    calc
  } = token;
  const tableBorder = `${unit(lineWidth)} ${lineType} ${tableBorderColor}`;
  return {
    [`${componentCls}-wrapper`]: Object.assign(Object.assign({
      clear: 'both',
      maxWidth: '100%'
    }, clearFix()), {
      [componentCls]: Object.assign(Object.assign({}, resetComponent(token)), {
        fontSize: tableFontSize,
        background: tableBg,
        borderRadius: `${unit(tableRadius)} ${unit(tableRadius)} 0 0`,
        // https://github.com/ant-design/ant-design/issues/47486
        scrollbarColor: `${token.tableScrollThumbBg} ${token.tableScrollBg}`
      }),
      // https://github.com/ant-design/ant-design/issues/17611
      table: {
        width: '100%',
        textAlign: 'start',
        borderRadius: `${unit(tableRadius)} ${unit(tableRadius)} 0 0`,
        borderCollapse: 'separate',
        borderSpacing: 0
      },
      // ============================= Cell ==============================
      [`
          ${componentCls}-cell,
          ${componentCls}-thead > tr > th,
          ${componentCls}-tbody > tr > th,
          ${componentCls}-tbody > tr > td,
          tfoot > tr > th,
          tfoot > tr > td
        `]: {
        position: 'relative',
        padding: `${unit(tablePaddingVertical)} ${unit(tablePaddingHorizontal)}`,
        overflowWrap: 'break-word'
      },
      // ============================ Title =============================
      [`${componentCls}-title`]: {
        padding: `${unit(tablePaddingVertical)} ${unit(tablePaddingHorizontal)}`
      },
      // ============================ Header ============================
      [`${componentCls}-thead`]: {
        [`
          > tr > th,
          > tr > td
        `]: {
          position: 'relative',
          color: tableHeaderTextColor,
          fontWeight: fontWeightStrong,
          textAlign: 'start',
          background: tableHeaderBg,
          borderBottom: tableBorder,
          transition: `background ${motionDurationMid} ease`,
          "&[colspan]:not([colspan='1'])": {
            textAlign: 'center'
          },
          [`&:not(:last-child):not(${componentCls}-selection-column):not(${componentCls}-row-expand-icon-cell):not([colspan])::before`]: {
            position: 'absolute',
            top: '50%',
            insetInlineEnd: 0,
            width: 1,
            height: '1.6em',
            backgroundColor: tableHeaderCellSplitColor,
            transform: 'translateY(-50%)',
            transition: `background-color ${motionDurationMid}`,
            content: '""'
          }
        },
        '> tr:not(:last-child) > th[colspan]': {
          borderBottom: 0
        }
      },
      // ============================ Body ============================
      [`${componentCls}-tbody`]: {
        '> tr': {
          [`> th, > td`]: {
            transition: `background ${motionDurationMid}, border-color ${motionDurationMid}`,
            borderBottom: tableBorder,
            // ========================= Nest Table ===========================
            [`
              > ${componentCls}-wrapper:only-child,
              > ${componentCls}-expanded-row-fixed > ${componentCls}-wrapper:only-child
            `]: {
              [componentCls]: {
                marginBlock: unit(calc(tablePaddingVertical).mul(-1).equal()),
                marginInline: `${unit(calc(tableExpandColumnWidth).sub(tablePaddingHorizontal).equal())}
                ${unit(calc(tablePaddingHorizontal).mul(-1).equal())}`,
                [`${componentCls}-tbody > tr:last-child > td`]: {
                  borderBottom: 0,
                  '&:first-child, &:last-child': {
                    borderRadius: 0
                  }
                }
              }
            }
          },
          '> th': {
            position: 'relative',
            color: tableHeaderTextColor,
            fontWeight: fontWeightStrong,
            textAlign: 'start',
            background: tableHeaderBg,
            borderBottom: tableBorder,
            transition: `background ${motionDurationMid} ease`
          }
        }
      },
      // ============================ Footer ============================
      [`${componentCls}-footer`]: {
        padding: `${unit(tablePaddingVertical)} ${unit(tablePaddingHorizontal)}`,
        color: tableFooterTextColor,
        background: tableFooterBg
      }
    })
  };
};
export const prepareComponentToken = token => {
  const {
    colorFillAlter,
    colorBgContainer,
    colorTextHeading,
    colorFillSecondary,
    colorFillContent,
    controlItemBgActive,
    controlItemBgActiveHover,
    padding,
    paddingSM,
    paddingXS,
    colorBorderSecondary,
    borderRadiusLG,
    controlHeight,
    colorTextPlaceholder,
    fontSize,
    fontSizeSM,
    lineHeight,
    lineWidth,
    colorIcon,
    colorIconHover,
    opacityLoading,
    controlInteractiveSize
  } = token;
  const colorFillSecondarySolid = new TinyColor(colorFillSecondary).onBackground(colorBgContainer).toHexShortString();
  const colorFillContentSolid = new TinyColor(colorFillContent).onBackground(colorBgContainer).toHexShortString();
  const colorFillAlterSolid = new TinyColor(colorFillAlter).onBackground(colorBgContainer).toHexShortString();
  const baseColorAction = new TinyColor(colorIcon);
  const baseColorActionHover = new TinyColor(colorIconHover);
  const expandIconHalfInner = controlInteractiveSize / 2 - lineWidth;
  const expandIconSize = expandIconHalfInner * 2 + lineWidth * 3;
  return {
    headerBg: colorFillAlterSolid,
    headerColor: colorTextHeading,
    headerSortActiveBg: colorFillSecondarySolid,
    headerSortHoverBg: colorFillContentSolid,
    bodySortBg: colorFillAlterSolid,
    rowHoverBg: colorFillAlterSolid,
    rowSelectedBg: controlItemBgActive,
    rowSelectedHoverBg: controlItemBgActiveHover,
    rowExpandedBg: colorFillAlter,
    cellPaddingBlock: padding,
    cellPaddingInline: padding,
    cellPaddingBlockMD: paddingSM,
    cellPaddingInlineMD: paddingXS,
    cellPaddingBlockSM: paddingXS,
    cellPaddingInlineSM: paddingXS,
    borderColor: colorBorderSecondary,
    headerBorderRadius: borderRadiusLG,
    footerBg: colorFillAlterSolid,
    footerColor: colorTextHeading,
    cellFontSize: fontSize,
    cellFontSizeMD: fontSize,
    cellFontSizeSM: fontSize,
    headerSplitColor: colorBorderSecondary,
    fixedHeaderSortActiveBg: colorFillSecondarySolid,
    headerFilterHoverBg: colorFillContent,
    filterDropdownMenuBg: colorBgContainer,
    filterDropdownBg: colorBgContainer,
    expandIconBg: colorBgContainer,
    selectionColumnWidth: controlHeight,
    stickyScrollBarBg: colorTextPlaceholder,
    stickyScrollBarBorderRadius: 100,
    expandIconMarginTop: (fontSize * lineHeight - lineWidth * 3) / 2 - Math.ceil((fontSizeSM * 1.4 - lineWidth * 3) / 2),
    headerIconColor: baseColorAction.clone().setAlpha(baseColorAction.getAlpha() * opacityLoading).toRgbString(),
    headerIconHoverColor: baseColorActionHover.clone().setAlpha(baseColorActionHover.getAlpha() * opacityLoading).toRgbString(),
    expandIconHalfInner,
    expandIconSize,
    expandIconScale: controlInteractiveSize / expandIconSize
  };
};
// ============================== Export ==============================
export default genStyleHooks('Table', token => {
  const {
    colorTextHeading,
    colorSplit,
    colorBgContainer,
    controlInteractiveSize: checkboxSize,
    headerBg,
    headerColor,
    headerSortActiveBg,
    headerSortHoverBg,
    bodySortBg,
    rowHoverBg,
    rowSelectedBg,
    rowSelectedHoverBg,
    rowExpandedBg,
    cellPaddingBlock,
    cellPaddingInline,
    cellPaddingBlockMD,
    cellPaddingInlineMD,
    cellPaddingBlockSM,
    cellPaddingInlineSM,
    borderColor,
    footerBg,
    footerColor,
    headerBorderRadius,
    cellFontSize,
    cellFontSizeMD,
    cellFontSizeSM,
    headerSplitColor,
    fixedHeaderSortActiveBg,
    headerFilterHoverBg,
    filterDropdownBg,
    expandIconBg,
    selectionColumnWidth,
    stickyScrollBarBg,
    calc
  } = token;
  const zIndexTableFixed = 2;
  const tableToken = mergeToken(token, {
    tableFontSize: cellFontSize,
    tableBg: colorBgContainer,
    tableRadius: headerBorderRadius,
    tablePaddingVertical: cellPaddingBlock,
    tablePaddingHorizontal: cellPaddingInline,
    tablePaddingVerticalMiddle: cellPaddingBlockMD,
    tablePaddingHorizontalMiddle: cellPaddingInlineMD,
    tablePaddingVerticalSmall: cellPaddingBlockSM,
    tablePaddingHorizontalSmall: cellPaddingInlineSM,
    tableBorderColor: borderColor,
    tableHeaderTextColor: headerColor,
    tableHeaderBg: headerBg,
    tableFooterTextColor: footerColor,
    tableFooterBg: footerBg,
    tableHeaderCellSplitColor: headerSplitColor,
    tableHeaderSortBg: headerSortActiveBg,
    tableHeaderSortHoverBg: headerSortHoverBg,
    tableBodySortBg: bodySortBg,
    tableFixedHeaderSortActiveBg: fixedHeaderSortActiveBg,
    tableHeaderFilterActiveBg: headerFilterHoverBg,
    tableFilterDropdownBg: filterDropdownBg,
    tableRowHoverBg: rowHoverBg,
    tableSelectedRowBg: rowSelectedBg,
    tableSelectedRowHoverBg: rowSelectedHoverBg,
    zIndexTableFixed,
    zIndexTableSticky: zIndexTableFixed + 1,
    tableFontSizeMiddle: cellFontSizeMD,
    tableFontSizeSmall: cellFontSizeSM,
    tableSelectionColumnWidth: selectionColumnWidth,
    tableExpandIconBg: expandIconBg,
    tableExpandColumnWidth: calc(checkboxSize).add(calc(token.padding).mul(2)).equal(),
    tableExpandedRowBg: rowExpandedBg,
    // Dropdown
    tableFilterDropdownWidth: 120,
    tableFilterDropdownHeight: 264,
    tableFilterDropdownSearchWidth: 140,
    // Virtual Scroll Bar
    tableScrollThumbSize: 8,
    // Mac scroll bar size
    tableScrollThumbBg: stickyScrollBarBg,
    tableScrollThumbBgHover: colorTextHeading,
    tableScrollBg: colorSplit
  });
  return [genTableStyle(tableToken), genPaginationStyle(tableToken), genSummaryStyle(tableToken), genSorterStyle(tableToken), genFilterStyle(tableToken), genBorderedStyle(tableToken), genRadiusStyle(tableToken), genExpandStyle(tableToken), genSummaryStyle(tableToken), genEmptyStyle(tableToken), genSelectionStyle(tableToken), genFixedStyle(tableToken), genStickyStyle(tableToken), genEllipsisStyle(tableToken), genSizeStyle(tableToken), genRtlStyle(tableToken), genVirtualStyle(tableToken)];
}, prepareComponentToken, {
  unitless: {
    expandIconScale: true
  }
});