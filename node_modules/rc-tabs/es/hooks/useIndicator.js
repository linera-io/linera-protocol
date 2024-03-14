import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import raf from "rc-util/es/raf";
import React, { useEffect, useRef, useState } from 'react';
var useIndicator = function useIndicator(options) {
  var activeTabOffset = options.activeTabOffset,
    horizontal = options.horizontal,
    rtl = options.rtl,
    _options$indicator = options.indicator,
    indicator = _options$indicator === void 0 ? {} : _options$indicator;
  var size = indicator.size,
    _indicator$align = indicator.align,
    align = _indicator$align === void 0 ? 'center' : _indicator$align;
  var _useState = useState(),
    _useState2 = _slicedToArray(_useState, 2),
    inkStyle = _useState2[0],
    setInkStyle = _useState2[1];
  var inkBarRafRef = useRef();
  var getLength = React.useCallback(function (origin) {
    if (typeof size === 'function') {
      return size(origin);
    }
    if (typeof size === 'number') {
      return size;
    }
    return origin;
  }, [size]);

  // Delay set ink style to avoid remove tab blink
  function cleanInkBarRaf() {
    raf.cancel(inkBarRafRef.current);
  }
  useEffect(function () {
    var newInkStyle = {};
    if (activeTabOffset) {
      if (horizontal) {
        newInkStyle.width = getLength(activeTabOffset.width);
        var key = rtl ? 'right' : 'left';
        if (align === 'start') {
          newInkStyle[key] = activeTabOffset[key];
        }
        if (align === 'center') {
          newInkStyle[key] = activeTabOffset[key] + activeTabOffset.width / 2;
          newInkStyle.transform = rtl ? 'translateX(50%)' : 'translateX(-50%)';
        }
        if (align === 'end') {
          newInkStyle[key] = activeTabOffset[key] + activeTabOffset.width;
          newInkStyle.transform = 'translateX(-100%)';
        }
      } else {
        newInkStyle.height = getLength(activeTabOffset.height);
        if (align === 'start') {
          newInkStyle.top = activeTabOffset.top;
        }
        if (align === 'center') {
          newInkStyle.top = activeTabOffset.top + activeTabOffset.height / 2;
          newInkStyle.transform = 'translateY(-50%)';
        }
        if (align === 'end') {
          newInkStyle.top = activeTabOffset.top + activeTabOffset.height;
          newInkStyle.transform = 'translateY(-100%)';
        }
      }
    }
    cleanInkBarRaf();
    inkBarRafRef.current = raf(function () {
      setInkStyle(newInkStyle);
    });
    return cleanInkBarRaf;
  }, [activeTabOffset, horizontal, rtl, align, getLength]);
  return {
    style: inkStyle
  };
};
export default useIndicator;