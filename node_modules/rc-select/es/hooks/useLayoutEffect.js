/* eslint-disable react-hooks/rules-of-hooks */
import * as React from 'react';
import { isBrowserClient } from "../utils/commonUtil";

/**
 * Wrap `React.useLayoutEffect` which will not throw warning message in test env
 */
export default function useLayoutEffect(effect, deps) {
  // Never happen in test env
  if (isBrowserClient) {
    /* istanbul ignore next */
    React.useLayoutEffect(effect, deps);
  } else {
    React.useEffect(effect, deps);
  }
}
/* eslint-enable */