import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import useMergedState from "rc-util/es/hooks/useMergedState";
var uniquePrefix = Math.random().toFixed(5).toString().slice(2);
var internalId = 0;
export default function useUUID(id) {
  var _useMergedState = useMergedState(id, {
      value: id
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    uuid = _useMergedState2[0],
    setUUID = _useMergedState2[1];
  React.useEffect(function () {
    internalId += 1;
    var newId = process.env.NODE_ENV === 'test' ? 'test' : "".concat(uniquePrefix, "-").concat(internalId);
    setUUID("rc-menu-uuid-".concat(newId));
  }, []);
  return uuid;
}