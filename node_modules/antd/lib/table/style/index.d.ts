/// <reference types="react" />
import type { FullToken, GetDefaultToken } from '../../theme/internal';
export interface ComponentToken {
    /**
     * @desc 表头背景
     * @descEN Background of table header
     */
    headerBg: string;
    /**
     * @desc 表头文字颜色
     * @descEN Color of table header text
     */
    headerColor: string;
    /**
     * @desc 表头排序激活态背景色
     * @descEN Background color of table header when sorted
     */
    headerSortActiveBg: string;
    /**
     * @desc 表头排序激活态悬浮背景色
     * @descEN Background color of table header when sorted and hovered
     */
    headerSortHoverBg: string;
    /**
     * @desc 表格排序列背景色
     * @descEN Background color of table sorted column
     */
    bodySortBg: string;
    /**
     * @desc 表格行悬浮背景色
     * @descEN Background color of table hovered row
     */
    rowHoverBg: string;
    /**
     * @desc 表格行选中背景色
     * @descEN Background color of table selected row
     */
    rowSelectedBg: string;
    /**
     * @desc 表格行选中悬浮背景色
     * @descEN Background color of table selected row when hovered
     */
    rowSelectedHoverBg: string;
    /**
     * @desc 表格行展开背景色
     * @descEN Background color of table expanded row
     */
    rowExpandedBg: string;
    /**
     * @desc 单元格纵向内间距
     * @descEN Vertical padding of table cell
     */
    cellPaddingBlock: number;
    /**
     * @desc 单元格横向内间距（默认大尺寸）
     * @descEN Horizontal padding of table cell (large size by default)
     */
    cellPaddingInline: number;
    /**
     * @desc 单元格纵向内间距（中等尺寸）
     * @descEN Vertical padding of table cell (middle size)
     */
    cellPaddingBlockMD: number;
    /**
     * @desc 单元格横向内间距（中等尺寸）
     * @descEN Horizontal padding of table cell (middle size)
     */
    cellPaddingInlineMD: number;
    /**
     * @desc 单元格纵向内间距（小尺寸）
     * @descEN Vertical padding of table cell (small size)
     */
    cellPaddingBlockSM: number;
    /**
     * @desc 单元格横向内间距（小尺寸）
     * @descEN Horizontal padding of table cell (small size)
     */
    cellPaddingInlineSM: number;
    /**
     * @desc 表格边框/分割线颜色
     * @descEN Border color of table
     */
    borderColor: string;
    /**
     * @desc 表头圆角
     * @descEN Border radius of table header
     */
    headerBorderRadius: number;
    /**
     * @desc 表格底部背景色
     * @descEN Background of footer
     */
    footerBg: string;
    /**
     * @desc 表格底部文字颜色
     * @descEN Color of footer text
     */
    footerColor: string;
    /**
     * @desc 单元格文字大小（默认大尺寸）
     * @descEN Font size of table cell (large size by default)
     */
    cellFontSize: number;
    /**
     * @desc 单元格文字大小（中等尺寸）
     * @descEN Font size of table cell (middle size)
     */
    cellFontSizeMD: number;
    /**
     * @desc 单元格文字大小（小尺寸）
     * @descEN Font size of table cell (small size)
     */
    cellFontSizeSM: number;
    /**
     * @desc 表头分割线颜色
     * @descEN Split border color of table header
     */
    headerSplitColor: string;
    /**
     * @desc 固定表头排序激活态背景色
     * @descEN Background color of fixed table header when sorted
     */
    fixedHeaderSortActiveBg: string;
    /**
     * @desc 表头过滤按钮悬浮背景色
     * @descEN Background color of table header filter button when hovered
     */
    headerFilterHoverBg: string;
    /**
     * @desc 过滤下拉菜单选项背景
     * @descEN Background of filter dropdown menu item
     */
    filterDropdownMenuBg: string;
    /**
     * @desc 过滤下拉菜单颜色
     * @descEN Color of filter dropdown
     */
    filterDropdownBg: string;
    /**
     * @desc 展开按钮背景色
     * @descEN Background of expand button
     */
    expandIconBg: string;
    /**
     * @desc 选择列宽度
     * @descEN Width of selection column
     */
    selectionColumnWidth: number;
    /**
     * @desc Sticky 模式下滚动条背景色
     * @descEN Background of sticky scrollbar
     */
    stickyScrollBarBg: string;
    /**
     * @desc Sticky 模式下滚动条圆角
     * @descEN Border radius of sticky scrollbar
     */
    stickyScrollBarBorderRadius: number;
}
export interface TableToken extends FullToken<'Table'> {
    tableFontSize: number;
    tableBg: string;
    tableRadius: number;
    tablePaddingHorizontal: number;
    tablePaddingVertical: number;
    tablePaddingHorizontalMiddle: number;
    tablePaddingVerticalMiddle: number;
    tablePaddingHorizontalSmall: number;
    tablePaddingVerticalSmall: number;
    tableBorderColor: string;
    tableHeaderTextColor: string;
    tableHeaderBg: string;
    tableFooterTextColor: string;
    tableFooterBg: string;
    tableHeaderCellSplitColor: string;
    tableHeaderSortBg: string;
    tableHeaderSortHoverBg: string;
    tableBodySortBg: string;
    tableFixedHeaderSortActiveBg: string;
    tableHeaderFilterActiveBg: string;
    tableFilterDropdownBg: string;
    tableFilterDropdownHeight: number;
    tableRowHoverBg: string;
    tableSelectedRowBg: string;
    tableSelectedRowHoverBg: string;
    tableFontSizeMiddle: number;
    tableFontSizeSmall: number;
    tableSelectionColumnWidth: number;
    tableExpandIconBg: string;
    tableExpandColumnWidth: number | string;
    tableExpandedRowBg: string;
    tableFilterDropdownWidth: number;
    tableFilterDropdownSearchWidth: number;
    zIndexTableFixed: number;
    zIndexTableSticky: number | string;
    tableScrollThumbSize: number;
    tableScrollThumbBg: string;
    tableScrollThumbBgHover: string;
    tableScrollBg: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Table'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
