import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";

/**
 * Trigger only when component unmount
 */
export default function useUnmount(triggerStart, triggerEnd) {
  var _React$useState = React.useState(false),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    firstMount = _React$useState2[0],
    setFirstMount = _React$useState2[1];
  useLayoutEffect(function () {
    if (firstMount) {
      triggerStart();
      return function () {
        triggerEnd();
      };
    }
  }, [firstMount]);
  useLayoutEffect(function () {
    setFirstMount(true);
    return function () {
      setFirstMount(false);
    };
  }, []);
}