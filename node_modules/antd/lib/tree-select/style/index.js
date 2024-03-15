"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useTreeSelectStyle;
exports.prepareComponentToken = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _style = require("../../checkbox/style");
var _internal = require("../../theme/internal");
var _style2 = require("../../tree/style");
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
      padding: `${(0, _cssinjs.unit)(token.paddingXS)} ${(0, _cssinjs.unit)(token.calc(token.paddingXS).div(2).equal())}`
    },
    // ====================== Tree ======================
    (0, _style2.genTreeStyle)(treePrefixCls, (0, _internal.mergeToken)(token, {
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
    (0, _style.getStyle)(`${treePrefixCls}-checkbox`, token),
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
const prepareComponentToken = exports.prepareComponentToken = _style2.initComponentToken;
// ============================== Export ==============================
function useTreeSelectStyle(prefixCls, treePrefixCls, rootCls) {
  return (0, _internal.genStyleHooks)('TreeSelect', token => {
    const treeSelectToken = (0, _internal.mergeToken)(token, {
      treePrefixCls
    });
    return [genBaseStyle(treeSelectToken)];
  }, _style2.initComponentToken)(prefixCls, rootCls);
}