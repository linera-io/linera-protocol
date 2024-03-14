import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useState, useRef, useEffect } from 'react';
import addEventListener from "rc-util/es/Dom/addEventListener";
import getFixScaleEleTransPosition from "../getFixScaleEleTransPosition";
function getDistance(a, b) {
  var x = a.x - b.x;
  var y = a.y - b.y;
  return Math.hypot(x, y);
}
function getCenter(oldPoint1, oldPoint2, newPoint1, newPoint2) {
  // Calculate the distance each point has moved
  var distance1 = getDistance(oldPoint1, newPoint1);
  var distance2 = getDistance(oldPoint2, newPoint2);

  // If both distances are 0, return the original points
  if (distance1 === 0 && distance2 === 0) {
    return [oldPoint1.x, oldPoint1.y];
  }

  // Calculate the ratio of the distances
  var ratio = distance1 / (distance1 + distance2);

  // Calculate the new center point based on the ratio
  var x = oldPoint1.x + ratio * (oldPoint2.x - oldPoint1.x);
  var y = oldPoint1.y + ratio * (oldPoint2.y - oldPoint1.y);
  return [x, y];
}
export default function useTouchEvent(imgRef, movable, visible, minScale, transform, updateTransform, dispatchZoomChange) {
  var rotate = transform.rotate,
    scale = transform.scale,
    x = transform.x,
    y = transform.y;
  var _useState = useState(false),
    _useState2 = _slicedToArray(_useState, 2),
    isTouching = _useState2[0],
    setIsTouching = _useState2[1];
  var touchPointInfo = useRef({
    point1: {
      x: 0,
      y: 0
    },
    point2: {
      x: 0,
      y: 0
    },
    eventType: 'none'
  });
  var updateTouchPointInfo = function updateTouchPointInfo(values) {
    touchPointInfo.current = _objectSpread(_objectSpread({}, touchPointInfo.current), values);
  };
  var onTouchStart = function onTouchStart(event) {
    if (!movable) return;
    event.stopPropagation();
    setIsTouching(true);
    var _event$touches = event.touches,
      touches = _event$touches === void 0 ? [] : _event$touches;
    if (touches.length > 1) {
      // touch zoom
      updateTouchPointInfo({
        point1: {
          x: touches[0].clientX,
          y: touches[0].clientY
        },
        point2: {
          x: touches[1].clientX,
          y: touches[1].clientY
        },
        eventType: 'touchZoom'
      });
    } else {
      // touch move
      updateTouchPointInfo({
        point1: {
          x: touches[0].clientX - x,
          y: touches[0].clientY - y
        },
        eventType: 'move'
      });
    }
  };
  var onTouchMove = function onTouchMove(event) {
    var _event$touches2 = event.touches,
      touches = _event$touches2 === void 0 ? [] : _event$touches2;
    var _touchPointInfo$curre = touchPointInfo.current,
      point1 = _touchPointInfo$curre.point1,
      point2 = _touchPointInfo$curre.point2,
      eventType = _touchPointInfo$curre.eventType;
    if (touches.length > 1 && eventType === 'touchZoom') {
      // touch zoom
      var newPoint1 = {
        x: touches[0].clientX,
        y: touches[0].clientY
      };
      var newPoint2 = {
        x: touches[1].clientX,
        y: touches[1].clientY
      };
      var _getCenter = getCenter(point1, point2, newPoint1, newPoint2),
        _getCenter2 = _slicedToArray(_getCenter, 2),
        centerX = _getCenter2[0],
        centerY = _getCenter2[1];
      var ratio = getDistance(newPoint1, newPoint2) / getDistance(point1, point2);
      dispatchZoomChange(ratio, 'touchZoom', centerX, centerY, true);
      updateTouchPointInfo({
        point1: newPoint1,
        point2: newPoint2,
        eventType: 'touchZoom'
      });
    } else if (eventType === 'move') {
      // touch move
      updateTransform({
        x: touches[0].clientX - point1.x,
        y: touches[0].clientY - point1.y
      }, 'move');
      updateTouchPointInfo({
        eventType: 'move'
      });
    }
  };
  var onTouchEnd = function onTouchEnd() {
    if (!visible) return;
    if (isTouching) {
      setIsTouching(false);
    }
    updateTouchPointInfo({
      eventType: 'none'
    });
    if (minScale > scale) {
      /** When the scaling ratio is less than the minimum scaling ratio, reset the scaling ratio */
      return updateTransform({
        x: 0,
        y: 0,
        scale: minScale
      }, 'touchZoom');
    }
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
  };
  useEffect(function () {
    var onTouchMoveListener;
    if (visible && movable) {
      onTouchMoveListener = addEventListener(window, 'touchmove', function (e) {
        return e.preventDefault();
      }, {
        passive: false
      });
    }
    return function () {
      var _onTouchMoveListener;
      (_onTouchMoveListener = onTouchMoveListener) === null || _onTouchMoveListener === void 0 || _onTouchMoveListener.remove();
    };
  }, [visible, movable]);
  return {
    isTouching: isTouching,
    onTouchStart: onTouchStart,
    onTouchMove: onTouchMove,
    onTouchEnd: onTouchEnd
  };
}
;