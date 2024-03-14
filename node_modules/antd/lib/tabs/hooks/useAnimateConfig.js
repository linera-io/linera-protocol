"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useAnimateConfig;
var _motion = require("../../_util/motion");
const motion = {
  motionAppear: false,
  motionEnter: true,
  motionLeave: true
};
function useAnimateConfig(prefixCls) {
  let animated = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {
    inkBar: true,
    tabPane: false
  };
  let mergedAnimated;
  if (animated === false) {
    mergedAnimated = {
      inkBar: false,
      tabPane: false
    };
  } else if (animated === true) {
    mergedAnimated = {
      inkBar: true,
      tabPane: true
    };
  } else {
    mergedAnimated = Object.assign({
      inkBar: true
    }, typeof animated === 'object' ? animated : {});
  }
  if (mergedAnimated.tabPane) {
    mergedAnimated.tabPaneMotion = Object.assign(Object.assign({}, motion), {
      motionName: (0, _motion.getTransitionName)(prefixCls, 'switch')
    });
  }
  return mergedAnimated;
}