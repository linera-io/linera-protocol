import { useLayoutUpdateEffect } from "rc-util/es/hooks/useLayoutEffect";
import raf from "rc-util/es/raf";
import * as React from 'react';

/**
 * Trigger `callback` immediately when `condition` is `true`.
 * But trigger `callback` in next frame when `condition` is `false`.
 */
export default function useLockEffect(condition, callback) {
  var delayFrames = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 1;
  var callbackRef = React.useRef(callback);
  callbackRef.current = callback;
  useLayoutUpdateEffect(function () {
    if (condition) {
      callbackRef.current(condition);
    } else {
      var id = raf(function () {
        callbackRef.current(condition);
      }, delayFrames);
      return function () {
        raf.cancel(id);
      };
    }
  }, [condition]);
}