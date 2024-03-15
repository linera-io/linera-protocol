import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useEvent, useMergedState } from 'rc-util';
import raf from "rc-util/es/raf";
import React from 'react';

/**
 * Will be `true` immediately for next effect.
 * But will be `false` for a delay of effect.
 */
export default function useDelayState(value, defaultValue, onChange) {
  var _useMergedState = useMergedState(defaultValue, {
      value: value
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    state = _useMergedState2[0],
    setState = _useMergedState2[1];
  var nextValueRef = React.useRef(value);

  // ============================= Update =============================
  var rafRef = React.useRef();
  var cancelRaf = function cancelRaf() {
    raf.cancel(rafRef.current);
  };
  var doUpdate = useEvent(function () {
    setState(nextValueRef.current);
    if (onChange && state !== nextValueRef.current) {
      onChange(nextValueRef.current);
    }
  });
  var updateValue = useEvent(function (next, immediately) {
    cancelRaf();
    nextValueRef.current = next;
    if (next || immediately) {
      doUpdate();
    } else {
      rafRef.current = raf(doUpdate);
    }
  });
  React.useEffect(function () {
    return cancelRaf;
  }, []);
  return [state, updateValue];
}