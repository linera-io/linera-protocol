import _typeof from "@babel/runtime/helpers/esm/typeof";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
/* eslint-disable no-param-reassign */
import * as React from 'react';
import raf from "rc-util/es/raf";
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import { warning } from 'rc-util';
var MAX_TIMES = 10;
export default function useScrollTo(containerRef, data, heights, itemHeight, getKey, collectHeight, syncScrollTop, triggerFlash) {
  var scrollRef = React.useRef();
  var _React$useState = React.useState(null),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    syncState = _React$useState2[0],
    setSyncState = _React$useState2[1];

  // ========================== Sync Scroll ==========================
  useLayoutEffect(function () {
    if (syncState && syncState.times < MAX_TIMES) {
      // Never reach
      if (!containerRef.current) {
        setSyncState(function (ori) {
          return _objectSpread({}, ori);
        });
        return;
      }
      collectHeight();
      var targetAlign = syncState.targetAlign,
        originAlign = syncState.originAlign,
        index = syncState.index,
        offset = syncState.offset;
      var height = containerRef.current.clientHeight;
      var needCollectHeight = false;
      var newTargetAlign = targetAlign;
      var targetTop = null;

      // Go to next frame if height not exist
      if (height) {
        var mergedAlign = targetAlign || originAlign;

        // Get top & bottom
        var stackTop = 0;
        var itemTop = 0;
        var itemBottom = 0;
        var maxLen = Math.min(data.length - 1, index);
        for (var i = 0; i <= maxLen; i += 1) {
          var key = getKey(data[i]);
          itemTop = stackTop;
          var cacheHeight = heights.get(key);
          itemBottom = itemTop + (cacheHeight === undefined ? itemHeight : cacheHeight);
          stackTop = itemBottom;
        }

        // Check if need sync height (visible range has item not record height)
        var leftHeight = mergedAlign === 'top' ? offset : height - offset;
        for (var _i = maxLen; _i >= 0; _i -= 1) {
          var _key = getKey(data[_i]);
          var _cacheHeight = heights.get(_key);
          if (_cacheHeight === undefined) {
            needCollectHeight = true;
            break;
          }
          leftHeight -= _cacheHeight;
          if (leftHeight <= 0) {
            break;
          }
        }

        // Scroll to
        switch (mergedAlign) {
          case 'top':
            targetTop = itemTop - offset;
            break;
          case 'bottom':
            targetTop = itemBottom - height + offset;
            break;
          default:
            {
              var scrollTop = containerRef.current.scrollTop;
              var scrollBottom = scrollTop + height;
              if (itemTop < scrollTop) {
                newTargetAlign = 'top';
              } else if (itemBottom > scrollBottom) {
                newTargetAlign = 'bottom';
              }
            }
        }
        if (targetTop !== null) {
          syncScrollTop(targetTop);
        }

        // One more time for sync
        if (targetTop !== syncState.lastTop) {
          needCollectHeight = true;
        }
      }

      // Trigger next effect
      if (needCollectHeight) {
        setSyncState(_objectSpread(_objectSpread({}, syncState), {}, {
          times: syncState.times + 1,
          targetAlign: newTargetAlign,
          lastTop: targetTop
        }));
      }
    } else if (process.env.NODE_ENV !== 'production' && (syncState === null || syncState === void 0 ? void 0 : syncState.times) === MAX_TIMES) {
      warning(false, 'Seems `scrollTo` with `rc-virtual-list` reach the max limitation. Please fire issue for us. Thanks.');
    }
  }, [syncState, containerRef.current]);

  // =========================== Scroll To ===========================
  return function (arg) {
    // When not argument provided, we think dev may want to show the scrollbar
    if (arg === null || arg === undefined) {
      triggerFlash();
      return;
    }

    // Normal scroll logic
    raf.cancel(scrollRef.current);
    if (typeof arg === 'number') {
      syncScrollTop(arg);
    } else if (arg && _typeof(arg) === 'object') {
      var index;
      var align = arg.align;
      if ('index' in arg) {
        index = arg.index;
      } else {
        index = data.findIndex(function (item) {
          return getKey(item) === arg.key;
        });
      }
      var _arg$offset = arg.offset,
        offset = _arg$offset === void 0 ? 0 : _arg$offset;
      setSyncState({
        times: 0,
        index: index,
        offset: offset,
        originAlign: align
      });
    }
  };
}