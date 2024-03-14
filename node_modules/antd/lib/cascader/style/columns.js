"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _style = require("../../checkbox/style");
var _style2 = require("../../style");
const getColumnsStyle = token => {
  const {
    prefixCls,
    componentCls
  } = token;
  const cascaderMenuItemCls = `${componentCls}-menu-item`;
  const iconCls = `
  &${cascaderMenuItemCls}-expand ${cascaderMenuItemCls}-expand-icon,
  ${cascaderMenuItemCls}-loading-icon
`;
  return [
  // ==================== Checkbox ====================
  (0, _style.getStyle)(`${prefixCls}-checkbox`, token), {
    [componentCls]: {
      // ================== Checkbox ==================
      '&-checkbox': {
        top: 0,
        marginInlineEnd: token.paddingXS
      },
      // ==================== Menu ====================
      // >>> Menus
      '&-menus': {
        display: 'flex',
        flexWrap: 'nowrap',
        alignItems: 'flex-start',
        [`&${componentCls}-menu-empty`]: {
          [`${componentCls}-menu`]: {
            width: '100%',
            height: 'auto',
            [cascaderMenuItemCls]: {
              color: token.colorTextDisabled
            }
          }
        }
      },
      // >>> Menu
      '&-menu': {
        flexGrow: 1,
        flexShrink: 0,
        minWidth: token.controlItemWidth,
        height: token.dropdownHeight,
        margin: 0,
        padding: token.menuPadding,
        overflow: 'auto',
        verticalAlign: 'top',
        listStyle: 'none',
        '-ms-overflow-style': '-ms-autohiding-scrollbar',
        // https://github.com/ant-design/ant-design/issues/11857
        '&:not(:last-child)': {
          borderInlineEnd: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.colorSplit}`
        },
        '&-item': Object.assign(Object.assign({}, _style2.textEllipsis), {
          display: 'flex',
          flexWrap: 'nowrap',
          alignItems: 'center',
          padding: token.optionPadding,
          lineHeight: token.lineHeight,
          cursor: 'pointer',
          transition: `all ${token.motionDurationMid}`,
          borderRadius: token.borderRadiusSM,
          '&:hover': {
            background: token.controlItemBgHover
          },
          '&-disabled': {
            color: token.colorTextDisabled,
            cursor: 'not-allowed',
            '&:hover': {
              background: 'transparent'
            },
            [iconCls]: {
              color: token.colorTextDisabled
            }
          },
          [`&-active:not(${cascaderMenuItemCls}-disabled)`]: {
            [`&, &:hover`]: {
              fontWeight: token.optionSelectedFontWeight,
              backgroundColor: token.optionSelectedBg
            }
          },
          '&-content': {
            flex: 'auto'
          },
          [iconCls]: {
            marginInlineStart: token.paddingXXS,
            color: token.colorTextDescription,
            fontSize: token.fontSizeIcon
          },
          '&-keyword': {
            color: token.colorHighlight
          }
        })
      }
    }
  }];
};
var _default = exports.default = getColumnsStyle;