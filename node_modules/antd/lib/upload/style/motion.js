"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _motion = require("../../style/motion");
const uploadAnimateInlineIn = new _cssinjs.Keyframes('uploadAnimateInlineIn', {
  from: {
    width: 0,
    height: 0,
    margin: 0,
    padding: 0,
    opacity: 0
  }
});
const uploadAnimateInlineOut = new _cssinjs.Keyframes('uploadAnimateInlineOut', {
  to: {
    width: 0,
    height: 0,
    margin: 0,
    padding: 0,
    opacity: 0
  }
});
// =========================== Motion ===========================
const genMotionStyle = token => {
  const {
    componentCls
  } = token;
  const inlineCls = `${componentCls}-animate-inline`;
  return [{
    [`${componentCls}-wrapper`]: {
      [`${inlineCls}-appear, ${inlineCls}-enter, ${inlineCls}-leave`]: {
        animationDuration: token.motionDurationSlow,
        animationTimingFunction: token.motionEaseInOutCirc,
        animationFillMode: 'forwards'
      },
      [`${inlineCls}-appear, ${inlineCls}-enter`]: {
        animationName: uploadAnimateInlineIn
      },
      [`${inlineCls}-leave`]: {
        animationName: uploadAnimateInlineOut
      }
    }
  }, {
    [`${componentCls}-wrapper`]: (0, _motion.initFadeMotion)(token)
  }, uploadAnimateInlineIn, uploadAnimateInlineOut];
};
var _default = exports.default = genMotionStyle;