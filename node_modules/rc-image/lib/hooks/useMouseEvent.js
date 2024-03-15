"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useMouseEvent;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = require("react");
var _addEventListener = _interopRequireDefault(require("rc-util/lib/Dom/addEventListener"));
var _warning = require("rc-util/lib/warning");
var _getFixScaleEleTransPosition = _interopRequireDefault(require("../getFixScaleEleTransPosition"));
var _previewConfig = require("../previewConfig");
function useMouseEvent(imgRef, movable, visible, scaleStep, transform, updateTransform, dispatchZoomChange) {
  var rotate = transform.rotate,
    scale = transform.scale,
    x = transform.x,
    y = transform.y;
  var _useState = (0, _react.useState)(false),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    isMoving = _useState2[0],
    setMoving = _useState2[1];
  var startPositionInfo = (0, _react.useRef)({
    diffX: 0,
    diffY: 0,
    transformX: 0,
    transformY: 0
  });
  var onMouseDown = function onMouseDown(event) {
    // Only allow main button
    if (!movable || event.button !== 0) return;
    event.preventDefault();
    event.stopPropagation();
    startPositionInfo.current = {
      diffX: event.pageX - x,
      diffY: event.pageY - y,
      transformX: x,
      transformY: y
    };
    setMoving(true);
  };
  var onMouseMove = function onMouseMove(event) {
    if (visible && isMoving) {
      updateTransform({
        x: event.pageX - startPositionInfo.current.diffX,
        y: event.pageY - startPositionInfo.current.diffY
      }, 'move');
    }
  };
  var onMouseUp = function onMouseUp() {
    if (visible && isMoving) {
      setMoving(false);

      /** No need to restore the position when the picture is not moved, So as not to interfere with the click */
      var _startPositionInfo$cu = startPositionInfo.current,
        transformX = _startPositionInfo$cu.transformX,
        transformY = _startPositionInfo$cu.transformY;
      var hasChangedPosition = x !== transformX && y !== transformY;
      if (!hasChangedPosition) return;
      var width = imgRef.current.offsetWidth * scale;
      var height = imgRef.current.offsetHeight * scale;
      // eslint-disable-next-line @typescript-eslint/no-shadow
      var _imgRef$current$getBo = imgRef.current.getBoundingClientRect(),
        left = _imgRef$current$getBo.left,
        top = _imgRef$current$getBo.top;
      var isRotate = rotate % 180 !== 0;
      var fixState = (0, _getFixScaleEleTransPosition.default)(isRotate ? height : width, isRotate ? width : height, left, top);
      if (fixState) {
        updateTransform((0, _objectSpread2.default)({}, fixState), 'dragRebound');
      }
    }
  };
  var onWheel = function onWheel(event) {
    if (!visible || event.deltaY == 0) return;
    // Scale ratio depends on the deltaY size
    var scaleRatio = Math.abs(event.deltaY / 100);
    // Limit the maximum scale ratio
    var mergedScaleRatio = Math.min(scaleRatio, _previewConfig.WHEEL_MAX_SCALE_RATIO);
    // Scale the ratio each time
    var ratio = _previewConfig.BASE_SCALE_RATIO + mergedScaleRatio * scaleStep;
    if (event.deltaY > 0) {
      ratio = _previewConfig.BASE_SCALE_RATIO / ratio;
    }
    dispatchZoomChange(ratio, 'wheel', event.clientX, event.clientY);
  };
  (0, _react.useEffect)(function () {
    var onTopMouseUpListener;
    var onTopMouseMoveListener;
    var onMouseUpListener;
    var onMouseMoveListener;
    if (movable) {
      onMouseUpListener = (0, _addEventListener.default)(window, 'mouseup', onMouseUp, false);
      onMouseMoveListener = (0, _addEventListener.default)(window, 'mousemove', onMouseMove, false);
      try {
        // Resolve if in iframe lost event
        /* istanbul ignore next */
        if (window.top !== window.self) {
          onTopMouseUpListener = (0, _addEventListener.default)(window.top, 'mouseup', onMouseUp, false);
          onTopMouseMoveListener = (0, _addEventListener.default)(window.top, 'mousemove', onMouseMove, false);
        }
      } catch (error) {
        /* istanbul ignore next */
        (0, _warning.warning)(false, "[rc-image] ".concat(error));
      }
    }
    return function () {
      var _onMouseUpListener, _onMouseMoveListener, _onTopMouseUpListener, _onTopMouseMoveListen;
      (_onMouseUpListener = onMouseUpListener) === null || _onMouseUpListener === void 0 || _onMouseUpListener.remove();
      (_onMouseMoveListener = onMouseMoveListener) === null || _onMouseMoveListener === void 0 || _onMouseMoveListener.remove();
      /* istanbul ignore next */
      (_onTopMouseUpListener = onTopMouseUpListener) === null || _onTopMouseUpListener === void 0 || _onTopMouseUpListener.remove();
      /* istanbul ignore next */
      (_onTopMouseMoveListen = onTopMouseMoveListener) === null || _onTopMouseMoveListen === void 0 || _onTopMouseMoveListen.remove();
    };
  }, [visible, isMoving, x, y, rotate, movable]);
  return {
    isMoving: isMoving,
    onMouseDown: onMouseDown,
    onMouseMove: onMouseMove,
    onMouseUp: onMouseUp,
    onWheel: onWheel
  };
}
;