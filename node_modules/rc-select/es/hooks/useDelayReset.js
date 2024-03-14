import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';

/**
 * Similar with `useLock`, but this hook will always execute last value.
 * When set to `true`, it will keep `true` for a short time even if `false` is set.
 */
export default function useDelayReset() {
  var timeout = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 10;
  var _React$useState = React.useState(false),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    bool = _React$useState2[0],
    setBool = _React$useState2[1];
  var delayRef = React.useRef(null);
  var cancelLatest = function cancelLatest() {
    window.clearTimeout(delayRef.current);
  };
  React.useEffect(function () {
    return cancelLatest;
  }, []);
  var delaySetBool = function delaySetBool(value, callback) {
    cancelLatest();
    delayRef.current = window.setTimeout(function () {
      setBool(value);
      if (callback) {
        callback();
      }
    }, timeout);
  };
  return [bool, delaySetBool, cancelLatest];
}