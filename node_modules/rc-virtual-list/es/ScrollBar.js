import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import classNames from 'classnames';
import raf from "rc-util/es/raf";
function getPageXY(e, horizontal) {
  var obj = 'touches' in e ? e.touches[0] : e;
  return obj[horizontal ? 'pageX' : 'pageY'];
}
var ScrollBar = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    rtl = props.rtl,
    scrollOffset = props.scrollOffset,
    scrollRange = props.scrollRange,
    onStartMove = props.onStartMove,
    onStopMove = props.onStopMove,
    onScroll = props.onScroll,
    horizontal = props.horizontal,
    spinSize = props.spinSize,
    containerSize = props.containerSize,
    style = props.style,
    propsThumbStyle = props.thumbStyle;
  var _React$useState = React.useState(false),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    dragging = _React$useState2[0],
    setDragging = _React$useState2[1];
  var _React$useState3 = React.useState(null),
    _React$useState4 = _slicedToArray(_React$useState3, 2),
    pageXY = _React$useState4[0],
    setPageXY = _React$useState4[1];
  var _React$useState5 = React.useState(null),
    _React$useState6 = _slicedToArray(_React$useState5, 2),
    startTop = _React$useState6[0],
    setStartTop = _React$useState6[1];
  var isLTR = !rtl;

  // ========================= Refs =========================
  var scrollbarRef = React.useRef();
  var thumbRef = React.useRef();

  // ======================= Visible ========================
  var _React$useState7 = React.useState(false),
    _React$useState8 = _slicedToArray(_React$useState7, 2),
    visible = _React$useState8[0],
    setVisible = _React$useState8[1];
  var visibleTimeoutRef = React.useRef();
  var delayHidden = function delayHidden() {
    clearTimeout(visibleTimeoutRef.current);
    setVisible(true);
    visibleTimeoutRef.current = setTimeout(function () {
      setVisible(false);
    }, 3000);
  };

  // ======================== Range =========================
  var enableScrollRange = scrollRange - containerSize || 0;
  var enableOffsetRange = containerSize - spinSize || 0;

  // ========================= Top ==========================
  var top = React.useMemo(function () {
    if (scrollOffset === 0 || enableScrollRange === 0) {
      return 0;
    }
    var ptg = scrollOffset / enableScrollRange;
    return ptg * enableOffsetRange;
  }, [scrollOffset, enableScrollRange, enableOffsetRange]);

  // ====================== Container =======================
  var onContainerMouseDown = function onContainerMouseDown(e) {
    e.stopPropagation();
    e.preventDefault();
  };

  // ======================== Thumb =========================
  var stateRef = React.useRef({
    top: top,
    dragging: dragging,
    pageY: pageXY,
    startTop: startTop
  });
  stateRef.current = {
    top: top,
    dragging: dragging,
    pageY: pageXY,
    startTop: startTop
  };
  var onThumbMouseDown = function onThumbMouseDown(e) {
    setDragging(true);
    setPageXY(getPageXY(e, horizontal));
    setStartTop(stateRef.current.top);
    onStartMove();
    e.stopPropagation();
    e.preventDefault();
  };

  // ======================== Effect ========================

  // React make event as passive, but we need to preventDefault
  // Add event on dom directly instead.
  // ref: https://github.com/facebook/react/issues/9809
  React.useEffect(function () {
    var onScrollbarTouchStart = function onScrollbarTouchStart(e) {
      e.preventDefault();
    };
    var scrollbarEle = scrollbarRef.current;
    var thumbEle = thumbRef.current;
    scrollbarEle.addEventListener('touchstart', onScrollbarTouchStart);
    thumbEle.addEventListener('touchstart', onThumbMouseDown);
    return function () {
      scrollbarEle.removeEventListener('touchstart', onScrollbarTouchStart);
      thumbEle.removeEventListener('touchstart', onThumbMouseDown);
    };
  }, []);

  // Pass to effect
  var enableScrollRangeRef = React.useRef();
  enableScrollRangeRef.current = enableScrollRange;
  var enableOffsetRangeRef = React.useRef();
  enableOffsetRangeRef.current = enableOffsetRange;
  React.useEffect(function () {
    if (dragging) {
      var moveRafId;
      var onMouseMove = function onMouseMove(e) {
        var _stateRef$current = stateRef.current,
          stateDragging = _stateRef$current.dragging,
          statePageY = _stateRef$current.pageY,
          stateStartTop = _stateRef$current.startTop;
        raf.cancel(moveRafId);
        if (stateDragging) {
          var offset = getPageXY(e, horizontal) - statePageY;
          var newTop = stateStartTop;
          if (!isLTR && horizontal) {
            newTop -= offset;
          } else {
            newTop += offset;
          }
          var tmpEnableScrollRange = enableScrollRangeRef.current;
          var tmpEnableOffsetRange = enableOffsetRangeRef.current;
          var ptg = tmpEnableOffsetRange ? newTop / tmpEnableOffsetRange : 0;
          var newScrollTop = Math.ceil(ptg * tmpEnableScrollRange);
          newScrollTop = Math.max(newScrollTop, 0);
          newScrollTop = Math.min(newScrollTop, tmpEnableScrollRange);
          moveRafId = raf(function () {
            onScroll(newScrollTop, horizontal);
          });
        }
      };
      var onMouseUp = function onMouseUp() {
        setDragging(false);
        onStopMove();
      };
      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('touchmove', onMouseMove);
      window.addEventListener('mouseup', onMouseUp);
      window.addEventListener('touchend', onMouseUp);
      return function () {
        window.removeEventListener('mousemove', onMouseMove);
        window.removeEventListener('touchmove', onMouseMove);
        window.removeEventListener('mouseup', onMouseUp);
        window.removeEventListener('touchend', onMouseUp);
        raf.cancel(moveRafId);
      };
    }
  }, [dragging]);
  React.useEffect(function () {
    delayHidden();
  }, [scrollOffset]);

  // ====================== Imperative ======================
  React.useImperativeHandle(ref, function () {
    return {
      delayHidden: delayHidden
    };
  });

  // ======================== Render ========================
  var scrollbarPrefixCls = "".concat(prefixCls, "-scrollbar");
  var containerStyle = {
    position: 'absolute',
    visibility: visible ? null : 'hidden'
  };
  var thumbStyle = {
    position: 'absolute',
    background: 'rgba(0, 0, 0, 0.5)',
    borderRadius: 99,
    cursor: 'pointer',
    userSelect: 'none'
  };
  if (horizontal) {
    // Container
    containerStyle.height = 8;
    containerStyle.left = 0;
    containerStyle.right = 0;
    containerStyle.bottom = 0;

    // Thumb
    thumbStyle.height = '100%';
    thumbStyle.width = spinSize;
    if (isLTR) {
      thumbStyle.left = top;
    } else {
      thumbStyle.right = top;
    }
  } else {
    // Container
    containerStyle.width = 8;
    containerStyle.top = 0;
    containerStyle.bottom = 0;
    if (isLTR) {
      containerStyle.right = 0;
    } else {
      containerStyle.left = 0;
    }

    // Thumb
    thumbStyle.width = '100%';
    thumbStyle.height = spinSize;
    thumbStyle.top = top;
  }
  return /*#__PURE__*/React.createElement("div", {
    ref: scrollbarRef,
    className: classNames(scrollbarPrefixCls, _defineProperty(_defineProperty(_defineProperty({}, "".concat(scrollbarPrefixCls, "-horizontal"), horizontal), "".concat(scrollbarPrefixCls, "-vertical"), !horizontal), "".concat(scrollbarPrefixCls, "-visible"), visible)),
    style: _objectSpread(_objectSpread({}, containerStyle), style),
    onMouseDown: onContainerMouseDown,
    onMouseMove: delayHidden
  }, /*#__PURE__*/React.createElement("div", {
    ref: thumbRef,
    className: classNames("".concat(scrollbarPrefixCls, "-thumb"), _defineProperty({}, "".concat(scrollbarPrefixCls, "-thumb-moving"), dragging)),
    style: _objectSpread(_objectSpread({}, thumbStyle), propsThumbStyle),
    onMouseDown: onThumbMouseDown
  }));
});
if (process.env.NODE_ENV !== 'production') {
  ScrollBar.displayName = 'ScrollBar';
}
export default ScrollBar;