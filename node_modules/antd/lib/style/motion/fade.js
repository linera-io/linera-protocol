"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.initFadeMotion = exports.fadeOut = exports.fadeIn = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _motion = require("./motion");
const fadeIn = exports.fadeIn = new _cssinjs.Keyframes('antFadeIn', {
  '0%': {
    opacity: 0
  },
  '100%': {
    opacity: 1
  }
});
const fadeOut = exports.fadeOut = new _cssinjs.Keyframes('antFadeOut', {
  '0%': {
    opacity: 1
  },
  '100%': {
    opacity: 0
  }
});
const initFadeMotion = function (token) {
  let sameLevel = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  const {
    antCls
  } = token;
  const motionCls = `${antCls}-fade`;
  const sameLevelPrefix = sameLevel ? '&' : '';
  return [(0, _motion.initMotion)(motionCls, fadeIn, fadeOut, token.motionDurationMid, sameLevel), {
    [`
        ${sameLevelPrefix}${motionCls}-enter,
        ${sameLevelPrefix}${motionCls}-appear
      `]: {
      opacity: 0,
      animationTimingFunction: 'linear'
    },
    [`${sameLevelPrefix}${motionCls}-leave`]: {
      animationTimingFunction: 'linear'
    }
  }];
};
exports.initFadeMotion = initFadeMotion;