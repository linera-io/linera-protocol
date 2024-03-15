import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useRef, useState, useEffect } from 'react';
/**
 * Execute code before next frame but async
 */
export function useLayoutState(defaultState) {
  var stateRef = useRef(defaultState);
  var _useState = useState({}),
    _useState2 = _slicedToArray(_useState, 2),
    forceUpdate = _useState2[1];
  var lastPromiseRef = useRef(null);
  var updateBatchRef = useRef([]);
  function setFrameState(updater) {
    updateBatchRef.current.push(updater);
    var promise = Promise.resolve();
    lastPromiseRef.current = promise;
    promise.then(function () {
      if (lastPromiseRef.current === promise) {
        var prevBatch = updateBatchRef.current;
        var prevState = stateRef.current;
        updateBatchRef.current = [];
        prevBatch.forEach(function (batchUpdater) {
          stateRef.current = batchUpdater(stateRef.current);
        });
        lastPromiseRef.current = null;
        if (prevState !== stateRef.current) {
          forceUpdate({});
        }
      }
    });
  }
  useEffect(function () {
    return function () {
      lastPromiseRef.current = null;
    };
  }, []);
  return [stateRef.current, setFrameState];
}

/** Lock frame, when frame pass reset the lock. */
export function useTimeoutLock(defaultState) {
  var frameRef = useRef(defaultState || null);
  var timeoutRef = useRef();
  function cleanUp() {
    window.clearTimeout(timeoutRef.current);
  }
  function setState(newState) {
    frameRef.current = newState;
    cleanUp();
    timeoutRef.current = window.setTimeout(function () {
      frameRef.current = null;
      timeoutRef.current = undefined;
    }, 100);
  }
  function getState() {
    return frameRef.current;
  }
  useEffect(function () {
    return cleanUp;
  }, []);
  return [setState, getState];
}