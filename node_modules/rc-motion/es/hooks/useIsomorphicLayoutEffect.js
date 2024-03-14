import canUseDom from "rc-util/es/Dom/canUseDom";
import { useEffect, useLayoutEffect } from 'react';

// It's safe to use `useLayoutEffect` but the warning is annoying
var useIsomorphicLayoutEffect = canUseDom() ? useLayoutEffect : useEffect;
export default useIsomorphicLayoutEffect;