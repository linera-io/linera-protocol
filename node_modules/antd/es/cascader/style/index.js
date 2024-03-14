import { genCompactItemStyle } from '../../style/compact-item';
import { genStyleHooks } from '../../theme/internal';
import getColumnsStyle from './columns';
// =============================== Base ===============================
const genBaseStyle = token => {
  const {
    componentCls,
    antCls
  } = token;
  return [
  // =====================================================
  // ==                     Control                     ==
  // =====================================================
  {
    [componentCls]: {
      width: token.controlWidth
    }
  },
  // =====================================================
  // ==                      Popup                      ==
  // =====================================================
  {
    [`${componentCls}-dropdown`]: [{
      [`&${antCls}-select-dropdown`]: {
        padding: 0
      }
    }, getColumnsStyle(token)]
  },
  // =====================================================
  // ==                       RTL                       ==
  // =====================================================
  {
    [`${componentCls}-dropdown-rtl`]: {
      direction: 'rtl'
    }
  },
  // =====================================================
  // ==             Space Compact                       ==
  // =====================================================
  genCompactItemStyle(token)];
};
// ============================== Export ==============================
export const prepareComponentToken = token => {
  const itemPaddingVertical = Math.round((token.controlHeight - token.fontSize * token.lineHeight) / 2);
  return {
    controlWidth: 184,
    controlItemWidth: 111,
    dropdownHeight: 180,
    optionSelectedBg: token.controlItemBgActive,
    optionSelectedFontWeight: token.fontWeightStrong,
    optionPadding: `${itemPaddingVertical}px ${token.paddingSM}px`,
    menuPadding: token.paddingXXS
  };
};
export default genStyleHooks('Cascader', token => [genBaseStyle(token)], prepareComponentToken);