import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
// import canUseDom from 'rc-util/lib/Dom/canUseDom';
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import * as React from 'react';

// We need fully clone React function here
// to avoid webpack warning React 17 do not export `useId`
var fullClone = _objectSpread({}, React);
var useInsertionEffect = fullClone.useInsertionEffect;
/**
 * Polyfill `useInsertionEffect` for React < 18
 * @param renderEffect will be executed in `useMemo`, and do not have callback
 * @param effect will be executed in `useLayoutEffect`
 * @param deps
 */
var useInsertionEffectPolyfill = function useInsertionEffectPolyfill(renderEffect, effect, deps) {
  React.useMemo(renderEffect, deps);
  useLayoutEffect(function () {
    return effect(true);
  }, deps);
};

/**
 * Compatible `useInsertionEffect`
 * will use `useInsertionEffect` if React version >= 18,
 * otherwise use `useInsertionEffectPolyfill`.
 */
var useCompatibleInsertionEffect = useInsertionEffect ? function (renderEffect, effect, deps) {
  return useInsertionEffect(function () {
    renderEffect();
    return effect();
  }, deps);
} : useInsertionEffectPolyfill;
export default useCompatibleInsertionEffect;