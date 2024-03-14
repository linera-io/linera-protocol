import { unit } from '@ant-design/cssinjs';
import { getStyle as getCheckboxStyle } from '../../checkbox/style';
import { genStyleHooks, mergeToken } from '../../theme/internal';
import { genTreeStyle, initComponentToken } from '../../tree/style';
// =============================== Base ===============================
const genBaseStyle = token => {
  const {
    componentCls,
    treePrefixCls,
    colorBgElevated
  } = token;
  const treeCls = `.${treePrefixCls}`;
  return [
  // ======================================================
  // ==                     Dropdown                     ==
  // ======================================================
  {
    [`${componentCls}-dropdown`]: [{
      padding: `${unit(token.paddingXS)} ${unit(token.calc(token.paddingXS).div(2).equal())}`
    },
    // ====================== Tree ======================
    genTreeStyle(treePrefixCls, mergeToken(token, {
      colorBgContainer: colorBgElevated
    })), {
      [treeCls]: {
        borderRadius: 0,
        [`${treeCls}-list-holder-inner`]: {
          alignItems: 'stretch',
          [`${treeCls}-treenode`]: {
            [`${treeCls}-node-content-wrapper`]: {
              flex: 'auto'
            }
          }
        }
      }
    },
    // ==================== Checkbox ====================
    getCheckboxStyle(`${treePrefixCls}-checkbox`, token),
    // ====================== RTL =======================
    {
      '&-rtl': {
        direction: 'rtl',
        [`${treeCls}-switcher${treeCls}-switcher_close`]: {
          [`${treeCls}-switcher-icon svg`]: {
            transform: 'rotate(90deg)'
          }
        }
      }
    }]
  }];
};
export const prepareComponentToken = initComponentToken;
// ============================== Export ==============================
export default function useTreeSelectStyle(prefixCls, treePrefixCls, rootCls) {
  return genStyleHooks('TreeSelect', token => {
    const treeSelectToken = mergeToken(token, {
      treePrefixCls
    });
    return [genBaseStyle(treeSelectToken)];
  }, initComponentToken)(prefixCls, rootCls);
}