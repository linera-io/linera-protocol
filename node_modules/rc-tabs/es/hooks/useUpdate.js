import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useLayoutUpdateEffect } from "rc-util/es/hooks/useLayoutEffect";
import { useRef, useState } from 'react';

/**
 * Help to merge callback with `useLayoutEffect`.
 * One time will only trigger once.
 */
export default function useUpdate(callback) {
  var _useState = useState(0),
    _useState2 = _slicedToArray(_useState, 2),
    count = _useState2[0],
    setCount = _useState2[1];
  var effectRef = useRef(0);
  var callbackRef = useRef();
  callbackRef.current = callback;

  // Trigger on `useLayoutEffect`
  useLayoutUpdateEffect(function () {
    var _callbackRef$current;
    (_callbackRef$current = callbackRef.current) === null || _callbackRef$current === void 0 || _callbackRef$current.call(callbackRef);
  }, [count]);

  // Trigger to update count
  return function () {
    if (effectRef.current !== count) {
      return;
    }
    effectRef.current += 1;
    setCount(effectRef.current);
  };
}
export function useUpdateState(defaultState) {
  var batchRef = useRef([]);
  var _useState3 = useState({}),
    _useState4 = _slicedToArray(_useState3, 2),
    forceUpdate = _useState4[1];
  var state = useRef(typeof defaultState === 'function' ? defaultState() : defaultState);
  var flushUpdate = useUpdate(function () {
    var current = state.current;
    batchRef.current.forEach(function (callback) {
      current = callback(current);
    });
    batchRef.current = [];
    state.current = current;
    forceUpdate({});
  });
  function updater(callback) {
    batchRef.current.push(callback);
    flushUpdate();
  }
  return [state.current, updater];
}