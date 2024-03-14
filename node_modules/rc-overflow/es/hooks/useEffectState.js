import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import useEvent from "rc-util/es/hooks/useEvent";
import * as React from 'react';
import { unstable_batchedUpdates } from 'react-dom';
import channelUpdate from './channelUpdate';
/**
 * Batcher for record any `useEffectState` need update.
 */
export function useBatcher() {
  // Updater Trigger
  var updateFuncRef = React.useRef(null);
  // Notify update
  var notifyEffectUpdate = function notifyEffectUpdate(callback) {
    if (!updateFuncRef.current) {
      updateFuncRef.current = [];
      channelUpdate(function () {
        unstable_batchedUpdates(function () {
          updateFuncRef.current.forEach(function (fn) {
            fn();
          });
          updateFuncRef.current = null;
        });
      });
    }
    updateFuncRef.current.push(callback);
  };
  return notifyEffectUpdate;
}
/**
 * Trigger state update by `useLayoutEffect` to save perf.
 */
export default function useEffectState(notifyEffectUpdate, defaultValue) {
  // Value
  var _React$useState = React.useState(defaultValue),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    stateValue = _React$useState2[0],
    setStateValue = _React$useState2[1];
  // Set State
  var setEffectVal = useEvent(function (nextValue) {
    notifyEffectUpdate(function () {
      setStateValue(nextValue);
    });
  });
  return [stateValue, setEffectVal];
}