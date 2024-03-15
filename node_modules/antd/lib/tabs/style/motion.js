"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _motion = require("../../style/motion");
const genMotionStyle = token => {
  const {
    componentCls,
    motionDurationSlow
  } = token;
  return [{
    [componentCls]: {
      [`${componentCls}-switch`]: {
        '&-appear, &-enter': {
          transition: 'none',
          '&-start': {
            opacity: 0
          },
          '&-active': {
            opacity: 1,
            transition: `opacity ${motionDurationSlow}`
          }
        },
        '&-leave': {
          position: 'absolute',
          transition: 'none',
          inset: 0,
          '&-start': {
            opacity: 1
          },
          '&-active': {
            opacity: 0,
            transition: `opacity ${motionDurationSlow}`
          }
        }
      }
    }
  },
  // Follow code may reuse in other components
  [(0, _motion.initSlideMotion)(token, 'slide-up'), (0, _motion.initSlideMotion)(token, 'slide-down')]];
};
var _default = exports.default = genMotionStyle;