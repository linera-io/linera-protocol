import type { EffectCallback } from 'react';
import * as React from 'react';
type UseCompatibleInsertionEffect = (renderEffect: EffectCallback, effect: (polyfill?: boolean) => ReturnType<EffectCallback>, deps: React.DependencyList) => void;
/**
 * Compatible `useInsertionEffect`
 * will use `useInsertionEffect` if React version >= 18,
 * otherwise use `useInsertionEffectPolyfill`.
 */
declare const useCompatibleInsertionEffect: UseCompatibleInsertionEffect;
export default useCompatibleInsertionEffect;
