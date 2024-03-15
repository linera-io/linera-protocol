"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ = require(".");
var _internal = require("../../theme/internal");
// Style as status component

// ============================== Preset ==============================
const genPresetStyle = token => (0, _internal.genPresetColor)(token, (colorKey, _ref) => {
  let {
    textColor,
    lightBorderColor,
    lightColor,
    darkColor
  } = _ref;
  return {
    [`${token.componentCls}${token.componentCls}-${colorKey}`]: {
      color: textColor,
      background: lightColor,
      borderColor: lightBorderColor,
      // Inverse color
      '&-inverse': {
        color: token.colorTextLightSolid,
        background: darkColor,
        borderColor: darkColor
      },
      [`&${token.componentCls}-borderless`]: {
        borderColor: 'transparent'
      }
    }
  };
});
// ============================== Export ==============================
var _default = exports.default = (0, _internal.genSubStyleComponent)(['Tag', 'preset'], token => {
  const tagToken = (0, _.prepareToken)(token);
  return genPresetStyle(tagToken);
}, _.prepareComponentToken);