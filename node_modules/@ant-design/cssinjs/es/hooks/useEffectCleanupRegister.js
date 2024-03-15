import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import { warning } from "rc-util/es/warning";
import * as React from 'react';
var fullClone = _objectSpread({}, React);
var useInsertionEffect = fullClone.useInsertionEffect;

// DO NOT register functions in useEffect cleanup function, or functions that registered will never be called.
var useCleanupRegister = function useCleanupRegister(deps) {
  var effectCleanups = [];
  var cleanupFlag = false;
  function register(fn) {
    if (cleanupFlag) {
      if (process.env.NODE_ENV !== 'production') {
        warning(false, '[Ant Design CSS-in-JS] You are registering a cleanup function after unmount, which will not have any effect.');
      }
      return;
    }
    effectCleanups.push(fn);
  }
  React.useEffect(function () {
    // Compatible with strict mode
    cleanupFlag = false;
    return function () {
      cleanupFlag = true;
      if (effectCleanups.length) {
        effectCleanups.forEach(function (fn) {
          return fn();
        });
      }
    };
  }, deps);
  return register;
};
var useRun = function useRun() {
  return function (fn) {
    fn();
  };
};

// Only enable register in React 18
var useEffectCleanupRegister = typeof useInsertionEffect !== 'undefined' ? useCleanupRegister : useRun;
export default useEffectCleanupRegister;