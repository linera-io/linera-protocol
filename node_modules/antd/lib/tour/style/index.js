"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _tinycolor = require("@ctrl/tinycolor");
var _style = require("../../style");
var _placementArrow = _interopRequireWildcard(require("../../style/placementArrow"));
var _internal = require("../../theme/internal");
var _roundedArrow = require("../../style/roundedArrow");
var _cssinjs = require("@ant-design/cssinjs");
// =============================== Base ===============================
const genBaseStyle = token => {
  const {
    componentCls,
    lineHeight,
    padding,
    paddingXS,
    borderRadius,
    borderRadiusXS,
    colorPrimary,
    colorText,
    colorFill,
    indicatorHeight,
    indicatorWidth,
    boxShadowTertiary,
    tourZIndexPopup,
    fontSize,
    colorBgElevated,
    fontWeightStrong,
    marginXS,
    colorTextLightSolid,
    tourBorderRadius,
    colorWhite,
    primaryNextBtnHoverBg,
    closeBtnSize,
    motionDurationSlow,
    antCls,
    primaryPrevBtnBg
  } = token;
  return [{
    [componentCls]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      color: colorText,
      position: 'absolute',
      zIndex: tourZIndexPopup,
      display: 'block',
      visibility: 'visible',
      fontSize,
      lineHeight,
      width: 520,
      '--antd-arrow-background-color': colorBgElevated,
      '&-pure': {
        maxWidth: '100%',
        position: 'relative'
      },
      [`&${componentCls}-hidden`]: {
        display: 'none'
      },
      // ============================= panel content ============================
      [`${componentCls}-content`]: {
        position: 'relative'
      },
      [`${componentCls}-inner`]: {
        textAlign: 'start',
        textDecoration: 'none',
        borderRadius: tourBorderRadius,
        boxShadow: boxShadowTertiary,
        position: 'relative',
        backgroundColor: colorBgElevated,
        border: 'none',
        backgroundClip: 'padding-box',
        [`${componentCls}-close`]: {
          position: 'absolute',
          top: padding,
          insetInlineEnd: padding,
          color: token.colorIcon,
          outline: 'none',
          width: closeBtnSize,
          height: closeBtnSize,
          borderRadius: token.borderRadiusSM,
          transition: `background-color ${token.motionDurationMid}, color ${token.motionDurationMid}`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer',
          '&:hover': {
            color: token.colorIconHover,
            backgroundColor: token.closeBtnHoverBg
          }
        },
        [`${componentCls}-cover`]: {
          textAlign: 'center',
          padding: `${(0, _cssinjs.unit)(token.calc(padding).add(closeBtnSize).add(paddingXS).equal())} ${(0, _cssinjs.unit)(padding)} 0`,
          img: {
            width: '100%'
          }
        },
        [`${componentCls}-header`]: {
          padding: `${(0, _cssinjs.unit)(padding)} ${(0, _cssinjs.unit)(padding)} ${(0, _cssinjs.unit)(paddingXS)}`,
          [`${componentCls}-title`]: {
            lineHeight,
            fontSize,
            fontWeight: fontWeightStrong
          }
        },
        [`${componentCls}-description`]: {
          padding: `0 ${(0, _cssinjs.unit)(padding)}`,
          lineHeight,
          wordWrap: 'break-word'
        },
        [`${componentCls}-footer`]: {
          padding: `${(0, _cssinjs.unit)(paddingXS)} ${(0, _cssinjs.unit)(padding)} ${(0, _cssinjs.unit)(padding)}`,
          textAlign: 'end',
          borderRadius: `0 0 ${(0, _cssinjs.unit)(borderRadiusXS)} ${(0, _cssinjs.unit)(borderRadiusXS)}`,
          display: 'flex',
          [`${componentCls}-indicators`]: {
            display: 'inline-block',
            [`${componentCls}-indicator`]: {
              width: indicatorWidth,
              height: indicatorHeight,
              display: 'inline-block',
              borderRadius: '50%',
              background: colorFill,
              '&:not(:last-child)': {
                marginInlineEnd: indicatorHeight
              },
              '&-active': {
                background: colorPrimary
              }
            }
          },
          [`${componentCls}-buttons`]: {
            marginInlineStart: 'auto',
            [`${antCls}-btn`]: {
              marginInlineStart: marginXS
            }
          }
        }
      },
      // =============================  primary type  ===========================
      // `$` for panel, `&$` for pure panel
      [`${componentCls}-primary, &${componentCls}-primary`]: {
        '--antd-arrow-background-color': colorPrimary,
        [`${componentCls}-inner`]: {
          color: colorTextLightSolid,
          textAlign: 'start',
          textDecoration: 'none',
          backgroundColor: colorPrimary,
          borderRadius,
          boxShadow: boxShadowTertiary,
          [`${componentCls}-close`]: {
            color: colorTextLightSolid
          },
          [`${componentCls}-indicators`]: {
            [`${componentCls}-indicator`]: {
              background: primaryPrevBtnBg,
              '&-active': {
                background: colorTextLightSolid
              }
            }
          },
          [`${componentCls}-prev-btn`]: {
            color: colorTextLightSolid,
            borderColor: primaryPrevBtnBg,
            backgroundColor: colorPrimary,
            '&:hover': {
              backgroundColor: primaryPrevBtnBg,
              borderColor: 'transparent'
            }
          },
          [`${componentCls}-next-btn`]: {
            color: colorPrimary,
            borderColor: 'transparent',
            background: colorWhite,
            '&:hover': {
              background: primaryNextBtnHoverBg
            }
          }
        }
      }
    }),
    // ============================= mask ===========================
    [`${componentCls}-mask`]: {
      [`${componentCls}-placeholder-animated`]: {
        transition: `all ${motionDurationSlow}`
      }
    },
    // =========== Limit left and right placement radius ==============
    [['&-placement-left', '&-placement-leftTop', '&-placement-leftBottom', '&-placement-right', '&-placement-rightTop', '&-placement-rightBottom'].join(',')]: {
      [`${componentCls}-inner`]: {
        borderRadius: token.min(tourBorderRadius, _placementArrow.MAX_VERTICAL_CONTENT_RADIUS)
      }
    }
  },
  // ============================= Arrow ===========================
  (0, _placementArrow.default)(token, 'var(--antd-arrow-background-color)')];
};
// ============================== Export ==============================
const prepareComponentToken = token => Object.assign(Object.assign({
  zIndexPopup: token.zIndexPopupBase + 70,
  closeBtnSize: token.fontSize * token.lineHeight,
  primaryPrevBtnBg: new _tinycolor.TinyColor(token.colorTextLightSolid).setAlpha(0.15).toRgbString(),
  closeBtnHoverBg: token.wireframe ? 'transparent' : token.colorFillContent,
  primaryNextBtnHoverBg: new _tinycolor.TinyColor(token.colorBgTextHover).onBackground(token.colorWhite).toRgbString()
}, (0, _placementArrow.getArrowOffsetToken)({
  contentRadius: token.borderRadiusLG,
  limitVerticalRadius: true
})), (0, _roundedArrow.getArrowToken)(token));
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Tour', token => {
  const {
    borderRadiusLG
  } = token;
  const TourToken = (0, _internal.mergeToken)(token, {
    indicatorWidth: 6,
    indicatorHeight: 6,
    tourBorderRadius: borderRadiusLG
  });
  return [genBaseStyle(TourToken)];
}, prepareComponentToken);