import type { CSSProperties } from 'react';
import type { FullToken, GetDefaultToken } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
    /**
     * @desc 弹出菜单的宽度
     * @descEN Width of popup menu
     */
    dropdownWidth: number;
    /**
     * @desc 弹出菜单的 z-index
     * @descEN z-index of popup menu
     */
    zIndexPopup: number;
    /** @deprecated Use `groupTitleColor` instead */
    colorGroupTitle: string;
    /**
     * @desc 分组标题文字颜色
     * @descEN Color of group title text
     */
    groupTitleColor: string;
    /**
     * @desc 分组标题文字高度
     * @descEN line-height of group title
     */
    groupTitleLineHeight: string | number;
    /**
     * @desc 分组标题文字大小
     * @descEN font-size of group title
     */
    groupTitleFontSize: number;
    /** @deprecated Use `itemBorderRadius` instead */
    radiusItem: number;
    /**
     * @desc 菜单项的圆角
     * @descEN Radius of menu item
     */
    itemBorderRadius: number;
    /** @deprecated Use `subMenuItemBorderRadius` instead */
    radiusSubMenuItem: number;
    /**
     * @desc 子菜单项的圆角
     * @descEN Radius of sub-menu item
     */
    subMenuItemBorderRadius: number;
    /** @deprecated Use `itemColor` instead */
    colorItemText: string;
    /**
     * @desc 菜单项文字颜色
     * @descEN Color of menu item text
     */
    itemColor: string;
    /** @deprecated Use `itemHoverColor` instead */
    colorItemTextHover: string;
    /**
     * @desc 菜单项文字悬浮颜色
     * @descEN Hover color of menu item text
     */
    itemHoverColor: string;
    /** @deprecated Use `horizontalItemHoverColor` instead */
    colorItemTextHoverHorizontal: string;
    /**
     * @desc 水平菜单项文字悬浮颜色
     * @descEN Hover color of horizontal menu item text
     */
    horizontalItemHoverColor: string;
    /** @deprecated Use `itemSelectedColor` instead */
    colorItemTextSelected: string;
    /**
     * @desc 菜单项文字选中颜色
     * @descEN Color of selected menu item text
     */
    itemSelectedColor: string;
    /** @deprecated Use `horizontalItemSelectedColor` instead */
    colorItemTextSelectedHorizontal: string;
    /**
     * @desc 水平菜单项文字选中颜色
     * @descEN Color of selected horizontal menu item text
     */
    horizontalItemSelectedColor: string;
    /** @deprecated Use `itemDisabledColor` instead */
    colorItemTextDisabled: string;
    /**
     * @desc 菜单项文字禁用颜色
     * @descEN Color of disabled menu item text
     */
    itemDisabledColor: string;
    /** @deprecated Use `dangerItemColor` instead */
    colorDangerItemText: string;
    /**
     * @desc 危险菜单项文字颜色
     * @descEN Color of danger menu item text
     */
    dangerItemColor: string;
    /** @deprecated Use `dangerItemHoverColor` instead */
    colorDangerItemTextHover: string;
    /**
     * @desc 危险菜单项文字悬浮颜色
     * @descEN Hover color of danger menu item text
     */
    dangerItemHoverColor: string;
    /** @deprecated Use `dangerItemSelectedColor` instead */
    colorDangerItemTextSelected: string;
    /**
     * @desc 危险菜单项文字选中颜色
     * @descEN Color of selected danger menu item text
     */
    dangerItemSelectedColor: string;
    /** @deprecated Use `dangerItemActiveBg` instead */
    colorDangerItemBgActive: string;
    /**
     * @desc 危险菜单项文字激活颜色
     * @descEN Color of active danger menu item text
     */
    dangerItemActiveBg: string;
    /** @deprecated Use `dangerItemSelectedBg` instead */
    colorDangerItemBgSelected: string;
    /**
     * @desc 危险菜单项文字选中颜色
     * @descEN Color of selected danger menu item text
     */
    dangerItemSelectedBg: string;
    /** @deprecated Use `itemBg` instead */
    colorItemBg: string;
    /**
     * @desc 菜单项背景色
     */
    itemBg: string;
    /** @deprecated Use `itemHoverBg` instead */
    colorItemBgHover: string;
    /**
     * @desc 菜单项悬浮态背景色
     * @descEN Background color of menu item when hover
     */
    itemHoverBg: string;
    /** @deprecated Use `subMenuItemBg` instead */
    colorSubItemBg: string;
    /**
     * @desc 子菜单项背景色
     * @descEN Background color of sub-menu item
     */
    subMenuItemBg: string;
    /** @deprecated Use `itemActiveBg` instead */
    colorItemBgActive: string;
    /**
     * @desc 菜单项激活态背景色
     * @descEN Background color of menu item when active
     */
    itemActiveBg: string;
    /** @deprecated Use `itemSelectedBg` instead */
    colorItemBgSelected: string;
    /**
     * @desc 菜单项选中态背景色
     * @descEN Background color of menu item when selected
     */
    itemSelectedBg: string;
    /** @deprecated Use `horizontalItemSelectedBg` instead */
    colorItemBgSelectedHorizontal: string;
    /**
     * @desc 水平菜单项选中态背景色
     * @descEN Background color of horizontal menu item when selected
     */
    horizontalItemSelectedBg: string;
    /** @deprecated Use `activeBarWidth` instead */
    colorActiveBarWidth: number;
    /**
     * @desc 菜单项指示条宽度
     * @descEN Width of menu item active bar
     */
    activeBarWidth: number;
    /** @deprecated Use `activeBarHeight` instead */
    colorActiveBarHeight: number;
    /**
     * @desc 菜单项指示条高度
     * @descEN Height of menu item active bar
     */
    activeBarHeight: number;
    /** @deprecated Use `activeBarBorderWidth` instead */
    colorActiveBarBorderSize: number;
    /**
     * @desc 菜单项指示条边框宽度
     * @descEN Border width of menu item active bar
     */
    activeBarBorderWidth: number;
    /**
     * @desc 菜单项横向外间距
     * @descEN Horizontal margin of menu item
     */
    itemMarginInline: number;
    /**
     * @desc 横向菜单项横悬浮态背景色
     * @descEN Background color of horizontal menu item when hover
     */
    horizontalItemHoverBg: string;
    /**
     * @desc 横向菜单项圆角
     * @descEN Border radius of horizontal menu item
     */
    horizontalItemBorderRadius: number;
    /**
     * @desc 菜单项高度
     * @descEN Height of menu item
     */
    itemHeight: number;
    /**
     * @desc 收起后的宽度
     * @descEN Width when collapsed
     */
    collapsedWidth: number;
    /**
     * @desc 弹出框背景色
     * @descEN Background color of popup
     */
    popupBg: string;
    /**
     * @desc 菜单项纵向外间距
     * @descEN margin-block of menu item
     */
    itemMarginBlock: CSSProperties['marginBlock'];
    /**
     * @desc 菜单项横向内间距
     * @descEN padding-inline of menu item
     */
    itemPaddingInline: CSSProperties['paddingInline'];
    /**
     * @desc 横向菜单行高
     * @descEN LineHeight of horizontal menu item
     */
    horizontalLineHeight: CSSProperties['lineHeight'];
    /**
     * @desc 图标与文字间距
     * @descEN Spacing between icon and text
     */
    iconMarginInlineEnd: CSSProperties['marginInlineEnd'];
    /**
     * @desc 图标尺寸
     * @descEN Size of icon
     */
    iconSize: number;
    /**
     * @desc 收起时图标尺寸
     * @descEN Size of icon when collapsed
     */
    collapsedIconSize: number;
    /**
     * @desc 暗色模式下的浮层菜单的背景颜色
     * @descEN The background color of the overlay menu in dark mode.
     */
    darkPopupBg: string;
    /**
     * @desc 暗色模式下的菜单项文字颜色
     * @descEN Color of menu item text in dark mode
     */
    darkItemColor: string;
    /**
     * @desc 暗色模式下的危险菜单项文字颜色
     * @descEN Color of danger menu item text in dark mode
     */
    darkDangerItemColor: string;
    /**
     * @desc 暗色模式下的菜单项背景
     * @descEN Background of menu item in dark mode
     */
    darkItemBg: string;
    /**
     * @desc 暗色模式下的子菜单项背景
     * @descEN Background of submenu item in dark mode
     */
    darkSubMenuItemBg: string;
    /**
     * @desc 暗色模式下的菜单项选中颜色
     * @descEN Color of selected menu item in dark mode
     */
    darkItemSelectedColor: string;
    /**
     * @desc 暗色模式下的菜单项选中背景
     * @descEN Background of active menu item in dark mode
     */
    darkItemSelectedBg: string;
    /**
     * @desc 暗色模式下的菜单项悬浮背景
     * @descEN Background of hovered menu item in dark mode
     */
    darkItemHoverBg: string;
    /**
     * @desc 暗色模式下的分组标题文字颜色
     * @descEN Color of group title text in dark mode
     */
    darkGroupTitleColor: string;
    /**
     * @desc 暗色模式下的菜单项悬浮颜色
     * @descEN Color of hovered menu item in dark mode
     */
    darkItemHoverColor: string;
    /**
     * @desc 暗色模式下的菜单项禁用颜色
     * @descEN Color of disabled menu item in dark mode
     */
    darkItemDisabledColor: string;
    /**
     * @desc 暗色模式下的危险菜单项选中背景
     * @descEN Background of active danger menu item in dark mode
     */
    darkDangerItemSelectedBg: string;
    /**
     * @desc 暗色模式下的危险菜单项悬浮文字背景
     * @descEN Background of hovered danger menu item in dark mode
     */
    darkDangerItemHoverColor: string;
    /**
     * @desc 暗色模式下的危险菜单项选中文字颜色
     * @descEN Color of selected danger menu item in dark mode
     */
    darkDangerItemSelectedColor: string;
    /**
     * @desc 暗色模式下的危险菜单项激活态背景
     * @descEN Background of active danger menu item in dark mode
     */
    darkDangerItemActiveBg: string;
}
export interface MenuToken extends FullToken<'Menu'> {
    menuHorizontalHeight: number | string;
    menuArrowSize: number | string;
    menuArrowOffset: number | string;
    menuSubMenuBg: string;
    darkPopupBg: string;
}
export declare const prepareComponentToken: GetDefaultToken<'Menu'>;
declare const _default: (prefixCls: string, rootCls?: string, injectStyle?: boolean) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
