import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useState, useRef, useEffect } from 'react';
import addEventListener from "rc-util/es/Dom/addEventListener";
import { warning } from "rc-util/es/warning";
import getFixScaleEleTransPosition from "../getFixScaleEleTransPosition";
import { BASE_SCALE_RATIO, WHEEL_MAX_SCALE_RATIO } from "../previewConfig";
export default function useMouseEvent(imgRef, movable, visible, scaleStep, transform, updateTransform, dispatchZoomChange) {
  var rotate = transform.rotate,
    scale = transform.scale,
    x = transform.x,
    y = transform.y;
  var _useState = useState(false),
    _useState2 = _slicedToArray(_useState, 2),
    isMoving = _useState2[0],
    setMoving = _useState2[1];
  var startPositionInfo = useRef({
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
      var fixState = getFixScaleEleTransPosition(isRotate ? height : width, isRotate ? width : height, left, top);
      if (fixState) {
        updateTransform(_objectSpread({}, fixState), 'dragRebound');
      }
    }
  };
  var onWheel = function onWheel(event) {
    if (!visible || event.deltaY == 0) return;
    // Scale ratio depends on the deltaY size
    var scaleRatio = Math.abs(event.deltaY / 100);
    // Limit the maximum scale ratio
    var mergedScaleRatio = Math.min(scaleRatio, WHEEL_MAX_SCALE_RATIO);
    // Scale the ratio each time
    var ratio = BASE_SCALE_RATIO + mergedScaleRatio * scaleStep;
    if (event.deltaY > 0) {
      ratio = BASE_SCALE_RATIO / ratio;
    }
    dispatchZoomChange(ratio, 'wheel', event.clientX, event.clientY);
  };
  useEffect(function () {
    var onTopMouseUpListener;
    var onTopMouseMoveListener;
    var onMouseUpListener;
    var onMouseMoveListener;
    if (movable) {
      onMouseUpListener = addEventListener(window, 'mouseup', onMouseUp, false);
      onMouseMoveListener = addEventListener(window, 'mousemove', onMouseMove, false);
      try {
        // Resolve if in iframe lost event
        /* istanbul ignore next */
        if (window.top !== window.self) {
          onTopMouseUpListener = addEventListener(window.top, 'mouseup', onMouseUp, false);
          onTopMouseMoveListener = addEventListener(window.top, 'mousemove', onMouseMove, false);
        }
      } catch (error) {
        /* istanbul ignore next */
        warning(false, "[rc-image] ".concat(error));
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