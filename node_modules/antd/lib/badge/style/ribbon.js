"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _ = require(".");
var _style = require("../../style");
var _internal = require("../../theme/internal");
// ============================== Ribbon ==============================
const genRibbonStyle = token => {
  const {
    antCls,
    badgeFontHeight,
    marginXS,
    badgeRibbonOffset,
    calc
  } = token;
  const ribbonPrefixCls = `${antCls}-ribbon`;
  const ribbonWrapperPrefixCls = `${antCls}-ribbon-wrapper`;
  const statusRibbonPreset = (0, _internal.genPresetColor)(token, (colorKey, _ref) => {
    let {
      darkColor
    } = _ref;
    return {
      [`&${ribbonPrefixCls}-color-${colorKey}`]: {
        background: darkColor,
        color: darkColor
      }
    };
  });
  return {
    [`${ribbonWrapperPrefixCls}`]: {
      position: 'relative'
    },
    [`${ribbonPrefixCls}`]: Object.assign(Object.assign(Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      position: 'absolute',
      top: marginXS,
      padding: `0 ${(0, _cssinjs.unit)(token.paddingXS)}`,
      color: token.colorPrimary,
      lineHeight: (0, _cssinjs.unit)(badgeFontHeight),
      whiteSpace: 'nowrap',
      backgroundColor: token.colorPrimary,
      borderRadius: token.borderRadiusSM,
      [`${ribbonPrefixCls}-text`]: {
        color: token.colorTextLightSolid
      },
      [`${ribbonPrefixCls}-corner`]: {
        position: 'absolute',
        top: '100%',
        width: badgeRibbonOffset,
        height: badgeRibbonOffset,
        color: 'currentcolor',
        border: `${(0, _cssinjs.unit)(calc(badgeRibbonOffset).div(2).equal())} solid`,
        transform: token.badgeRibbonCornerTransform,
        transformOrigin: 'top',
        filter: token.badgeRibbonCornerFilter
      }
    }), statusRibbonPreset), {
      [`&${ribbonPrefixCls}-placement-end`]: {
        insetInlineEnd: calc(badgeRibbonOffset).mul(-1).equal(),
        borderEndEndRadius: 0,
        [`${ribbonPrefixCls}-corner`]: {
          insetInlineEnd: 0,
          borderInlineEndColor: 'transparent',
          borderBlockEndColor: 'transparent'
        }
      },
      [`&${ribbonPrefixCls}-placement-start`]: {
        insetInlineStart: calc(badgeRibbonOffset).mul(-1).equal(),
        borderEndStartRadius: 0,
        [`${ribbonPrefixCls}-corner`]: {
          insetInlineStart: 0,
          borderBlockEndColor: 'transparent',
          borderInlineStartColor: 'transparent'
        }
      },
      // ====================== RTL =======================
      '&-rtl': {
        direction: 'rtl'
      }
    })
  };
};
// ============================== Export ==============================
var _default = exports.default = (0, _internal.genStyleHooks)(['Badge', 'Ribbon'], token => {
  const badgeToken = (0, _.prepareToken)(token);
  return genRibbonStyle(badgeToken);
}, _.prepareComponentToken);