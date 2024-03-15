"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _compactItem = require("../../style/compact-item");
var _internal = require("../../theme/internal");
var _columns = _interopRequireDefault(require("./columns"));
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
    }, (0, _columns.default)(token)]
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
  (0, _compactItem.genCompactItemStyle)(token)];
};
// ============================== Export ==============================
const prepareComponentToken = token => {
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
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Cascader', token => [genBaseStyle(token)], prepareComponentToken);