import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import canUseDom from "rc-util/es/Dom/canUseDom";
var uuid = 0;

/** Is client side and not jsdom */
export var isBrowserClient = process.env.NODE_ENV !== 'test' && canUseDom();

/** Get unique id for accessibility usage */
export function getUUID() {
  var retId;

  // Test never reach
  /* istanbul ignore if */
  if (isBrowserClient) {
    retId = uuid;
    uuid += 1;
  } else {
    retId = 'TEST_OR_SSR';
  }
  return retId;
}
export default function useId(id) {
  // Inner id for accessibility usage. Only work in client side
  var _React$useState = React.useState(),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    innerId = _React$useState2[0],
    setInnerId = _React$useState2[1];
  React.useEffect(function () {
    setInnerId("rc_select_".concat(getUUID()));
  }, []);
  return id || innerId;
}