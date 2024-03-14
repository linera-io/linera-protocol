"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useTarget;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _useEvent = _interopRequireDefault(require("rc-util/lib/hooks/useEvent"));
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _react = require("react");
var _util = require("../util");
function useTarget(target, open, gap, scrollIntoViewOptions) {
  // ========================= Target =========================
  // We trade `undefined` as not get target by function yet.
  // `null` as empty target.
  var _useState = (0, _react.useState)(undefined),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    targetElement = _useState2[0],
    setTargetElement = _useState2[1];
  (0, _useLayoutEffect.default)(function () {
    var nextElement = typeof target === 'function' ? target() : target;
    setTargetElement(nextElement || null);
  });

  // ========================= Align ==========================
  var _useState3 = (0, _react.useState)(null),
    _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
    posInfo = _useState4[0],
    setPosInfo = _useState4[1];
  var updatePos = (0, _useEvent.default)(function () {
    if (targetElement) {
      // Exist target element. We should scroll and get target position
      if (!(0, _util.isInViewPort)(targetElement) && open) {
        targetElement.scrollIntoView(scrollIntoViewOptions);
      }
      var _targetElement$getBou = targetElement.getBoundingClientRect(),
        left = _targetElement$getBou.left,
        top = _targetElement$getBou.top,
        width = _targetElement$getBou.width,
        height = _targetElement$getBou.height;
      var nextPosInfo = {
        left: left,
        top: top,
        width: width,
        height: height,
        radius: 0
      };
      setPosInfo(function (origin) {
        if (JSON.stringify(origin) !== JSON.stringify(nextPosInfo)) {
          return nextPosInfo;
        }
        return origin;
      });
    } else {
      // Not exist target which means we just show in center
      setPosInfo(null);
    }
  });
  var getGapOffset = function getGapOffset(index) {
    var _ref;
    return (_ref = Array.isArray(gap === null || gap === void 0 ? void 0 : gap.offset) ? gap === null || gap === void 0 ? void 0 : gap.offset[index] : gap === null || gap === void 0 ? void 0 : gap.offset) !== null && _ref !== void 0 ? _ref : 6;
  };
  (0, _useLayoutEffect.default)(function () {
    updatePos();
    // update when window resize
    window.addEventListener('resize', updatePos);
    return function () {
      window.removeEventListener('resize', updatePos);
    };
  }, [targetElement, open, updatePos]);

  // ======================== PosInfo =========================
  var mergedPosInfo = (0, _react.useMemo)(function () {
    if (!posInfo) {
      return posInfo;
    }
    var gapOffsetX = getGapOffset(0);
    var gapOffsetY = getGapOffset(1);
    var gapRadius = (gap === null || gap === void 0 ? void 0 : gap.radius) || 2;
    return {
      left: posInfo.left - gapOffsetX,
      top: posInfo.top - gapOffsetY,
      width: posInfo.width + gapOffsetX * 2,
      height: posInfo.height + gapOffsetY * 2,
      radius: gapRadius
    };
  }, [posInfo, gap]);
  return [mergedPosInfo, targetElement];
}