import * as React from 'react';

/**
 * Same as `React.useCallback` but always return a memoized function
 * but redirect to real function.
 */
export default function useRefFunc(callback) {
  var funcRef = React.useRef();
  funcRef.current = callback;
  var cacheFn = React.useCallback(function () {
    return funcRef.current.apply(funcRef, arguments);
  }, []);
  return cacheFn;
}