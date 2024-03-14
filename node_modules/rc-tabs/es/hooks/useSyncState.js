import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
export default function useSyncState(defaultState, onChange) {
  var stateRef = React.useRef(defaultState);
  var _React$useState = React.useState({}),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    forceUpdate = _React$useState2[1];
  function setState(updater) {
    var newValue = typeof updater === 'function' ? updater(stateRef.current) : updater;
    if (newValue !== stateRef.current) {
      onChange(newValue, stateRef.current);
    }
    stateRef.current = newValue;
    forceUpdate({});
  }
  return [stateRef.current, setState];
}