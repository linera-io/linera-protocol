import * as React from 'react';
import useForceUpdate from './useForceUpdate';
export default function useSyncState(initialValue) {
  const ref = React.useRef(initialValue);
  const forceUpdate = useForceUpdate();
  return [() => ref.current, newValue => {
    ref.current = newValue;
    // re-render
    forceUpdate();
  }];
}