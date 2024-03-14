import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';

/**
 * Sync value with state.
 * This should only used for internal which not affect outside calculation.
 * Since it's not safe for suspense.
 */
export default function useSyncState(defaultValue, controlledValue) {
  var valueRef = React.useRef(defaultValue);
  var _React$useState = React.useState({}),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    forceUpdate = _React$useState2[1];
  var getter = function getter(useControlledValueFirst) {
    return useControlledValueFirst && controlledValue !== undefined ? controlledValue : valueRef.current;
  };
  var setter = function setter(nextValue) {
    valueRef.current = nextValue;
    forceUpdate({});
  };
  return [getter, setter, getter(true)];
}