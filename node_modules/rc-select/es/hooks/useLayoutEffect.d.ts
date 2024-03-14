import * as React from 'react';
/**
 * Wrap `React.useLayoutEffect` which will not throw warning message in test env
 */
export default function useLayoutEffect(effect: React.EffectCallback, deps?: React.DependencyList): void;
