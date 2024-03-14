"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _useZIndex = require("../../_util/hooks/useZIndex");
var _style = require("../../style");
var _internal = require("../../theme/internal");
const genMessageStyle = token => {
  const {
    componentCls,
    iconCls,
    boxShadow,
    colorText,
    colorSuccess,
    colorError,
    colorWarning,
    colorInfo,
    fontSizeLG,
    motionEaseInOutCirc,
    motionDurationSlow,
    marginXS,
    paddingXS,
    borderRadiusLG,
    zIndexPopup,
    // Custom token
    contentPadding,
    contentBg
  } = token;
  const noticeCls = `${componentCls}-notice`;
  const messageMoveIn = new _cssinjs.Keyframes('MessageMoveIn', {
    '0%': {
      padding: 0,
      transform: 'translateY(-100%)',
      opacity: 0
    },
    '100%': {
      padding: paddingXS,
      transform: 'translateY(0)',
      opacity: 1
    }
  });
  const messageMoveOut = new _cssinjs.Keyframes('MessageMoveOut', {
    '0%': {
      maxHeight: token.height,
      padding: paddingXS,
      opacity: 1
    },
    '100%': {
      maxHeight: 0,
      padding: 0,
      opacity: 0
    }
  });
  const noticeStyle = {
    padding: paddingXS,
    textAlign: 'center',
    [`${componentCls}-custom-content > ${iconCls}`]: {
      verticalAlign: 'text-bottom',
      marginInlineEnd: marginXS,
      // affected by ltr or rtl
      fontSize: fontSizeLG
    },
    [`${noticeCls}-content`]: {
      display: 'inline-block',
      padding: contentPadding,
      background: contentBg,
      borderRadius: borderRadiusLG,
      boxShadow,
      pointerEvents: 'all'
    },
    [`${componentCls}-success > ${iconCls}`]: {
      color: colorSuccess
    },
    [`${componentCls}-error > ${iconCls}`]: {
      color: colorError
    },
    [`${componentCls}-warning > ${iconCls}`]: {
      color: colorWarning
    },
    [`${componentCls}-info > ${iconCls},
      ${componentCls}-loading > ${iconCls}`]: {
      color: colorInfo
    }
  };
  return [
  // ============================ Holder ============================
  {
    [componentCls]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      color: colorText,
      position: 'fixed',
      top: marginXS,
      width: '100%',
      pointerEvents: 'none',
      zIndex: zIndexPopup,
      [`${componentCls}-move-up`]: {
        animationFillMode: 'forwards'
      },
      [`
        ${componentCls}-move-up-appear,
        ${componentCls}-move-up-enter
      `]: {
        animationName: messageMoveIn,
        animationDuration: motionDurationSlow,
        animationPlayState: 'paused',
        animationTimingFunction: motionEaseInOutCirc
      },
      [`
        ${componentCls}-move-up-appear${componentCls}-move-up-appear-active,
        ${componentCls}-move-up-enter${componentCls}-move-up-enter-active
      `]: {
        animationPlayState: 'running'
      },
      [`${componentCls}-move-up-leave`]: {
        animationName: messageMoveOut,
        animationDuration: motionDurationSlow,
        animationPlayState: 'paused',
        animationTimingFunction: motionEaseInOutCirc
      },
      [`${componentCls}-move-up-leave${componentCls}-move-up-leave-active`]: {
        animationPlayState: 'running'
      },
      '&-rtl': {
        direction: 'rtl',
        span: {
          direction: 'rtl'
        }
      }
    })
  },
  // ============================ Notice ============================
  {
    [componentCls]: {
      [`${noticeCls}-wrapper`]: Object.assign({}, noticeStyle)
    }
  },
  // ============================= Pure =============================
  {
    [`${componentCls}-notice-pure-panel`]: Object.assign(Object.assign({}, noticeStyle), {
      padding: 0,
      textAlign: 'start'
    })
  }];
};
const prepareComponentToken = token => ({
  zIndexPopup: token.zIndexPopupBase + _useZIndex.CONTAINER_MAX_OFFSET + 10,
  contentBg: token.colorBgElevated,
  contentPadding: `${(token.controlHeightLG - token.fontSize * token.lineHeight) / 2}px ${token.paddingSM}px`
});
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Message', token => {
  // Gen-style functions here
  const combinedToken = (0, _internal.mergeToken)(token, {
    height: 150
  });
  return [genMessageStyle(combinedToken)];
}, prepareComponentToken);