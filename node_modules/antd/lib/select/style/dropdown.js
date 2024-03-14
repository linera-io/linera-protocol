"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _style = require("../../style");
var _motion = require("../../style/motion");
const genItemStyle = token => {
  const {
    optionHeight,
    optionFontSize,
    optionLineHeight,
    optionPadding
  } = token;
  return {
    position: 'relative',
    display: 'block',
    minHeight: optionHeight,
    padding: optionPadding,
    color: token.colorText,
    fontWeight: 'normal',
    fontSize: optionFontSize,
    lineHeight: optionLineHeight,
    boxSizing: 'border-box'
  };
};
const genSingleStyle = token => {
  const {
    antCls,
    componentCls
  } = token;
  const selectItemCls = `${componentCls}-item`;
  const slideUpEnterActive = `&${antCls}-slide-up-enter${antCls}-slide-up-enter-active`;
  const slideUpAppearActive = `&${antCls}-slide-up-appear${antCls}-slide-up-appear-active`;
  const slideUpLeaveActive = `&${antCls}-slide-up-leave${antCls}-slide-up-leave-active`;
  const dropdownPlacementCls = `${componentCls}-dropdown-placement-`;
  return [{
    [`${componentCls}-dropdown`]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      position: 'absolute',
      top: -9999,
      zIndex: token.zIndexPopup,
      boxSizing: 'border-box',
      padding: token.paddingXXS,
      overflow: 'hidden',
      fontSize: token.fontSize,
      // Fix select render lag of long text in chrome
      // https://github.com/ant-design/ant-design/issues/11456
      // https://github.com/ant-design/ant-design/issues/11843
      fontVariant: 'initial',
      backgroundColor: token.colorBgElevated,
      borderRadius: token.borderRadiusLG,
      outline: 'none',
      boxShadow: token.boxShadowSecondary,
      [`
          ${slideUpEnterActive}${dropdownPlacementCls}bottomLeft,
          ${slideUpAppearActive}${dropdownPlacementCls}bottomLeft
        `]: {
        animationName: _motion.slideUpIn
      },
      [`
          ${slideUpEnterActive}${dropdownPlacementCls}topLeft,
          ${slideUpAppearActive}${dropdownPlacementCls}topLeft,
          ${slideUpEnterActive}${dropdownPlacementCls}topRight,
          ${slideUpAppearActive}${dropdownPlacementCls}topRight
        `]: {
        animationName: _motion.slideDownIn
      },
      [`${slideUpLeaveActive}${dropdownPlacementCls}bottomLeft`]: {
        animationName: _motion.slideUpOut
      },
      [`
          ${slideUpLeaveActive}${dropdownPlacementCls}topLeft,
          ${slideUpLeaveActive}${dropdownPlacementCls}topRight
        `]: {
        animationName: _motion.slideDownOut
      },
      '&-hidden': {
        display: 'none'
      },
      [`${selectItemCls}`]: Object.assign(Object.assign({}, genItemStyle(token)), {
        cursor: 'pointer',
        transition: `background ${token.motionDurationSlow} ease`,
        borderRadius: token.borderRadiusSM,
        // =========== Group ============
        '&-group': {
          color: token.colorTextDescription,
          fontSize: token.fontSizeSM,
          cursor: 'default'
        },
        // =========== Option ===========
        '&-option': {
          display: 'flex',
          '&-content': Object.assign({
            flex: 'auto'
          }, _style.textEllipsis),
          '&-state': {
            flex: 'none',
            display: 'flex',
            alignItems: 'center'
          },
          [`&-active:not(${selectItemCls}-option-disabled)`]: {
            backgroundColor: token.optionActiveBg
          },
          [`&-selected:not(${selectItemCls}-option-disabled)`]: {
            color: token.optionSelectedColor,
            fontWeight: token.optionSelectedFontWeight,
            backgroundColor: token.optionSelectedBg,
            [`${selectItemCls}-option-state`]: {
              color: token.colorPrimary
            },
            [`&:has(+ ${selectItemCls}-option-selected:not(${selectItemCls}-option-disabled))`]: {
              borderEndStartRadius: 0,
              borderEndEndRadius: 0,
              [`& + ${selectItemCls}-option-selected:not(${selectItemCls}-option-disabled)`]: {
                borderStartStartRadius: 0,
                borderStartEndRadius: 0
              }
            }
          },
          '&-disabled': {
            [`&${selectItemCls}-option-selected`]: {
              backgroundColor: token.colorBgContainerDisabled
            },
            color: token.colorTextDisabled,
            cursor: 'not-allowed'
          },
          '&-grouped': {
            paddingInlineStart: token.calc(token.controlPaddingHorizontal).mul(2).equal()
          }
        },
        '&-empty': Object.assign(Object.assign({}, genItemStyle(token)), {
          color: token.colorTextDisabled
        })
      }),
      // =========================== RTL ===========================
      '&-rtl': {
        direction: 'rtl'
      }
    })
  },
  // Follow code may reuse in other components
  (0, _motion.initSlideMotion)(token, 'slide-up'), (0, _motion.initSlideMotion)(token, 'slide-down'), (0, _motion.initMoveMotion)(token, 'move-up'), (0, _motion.initMoveMotion)(token, 'move-down')];
};
var _default = exports.default = genSingleStyle;