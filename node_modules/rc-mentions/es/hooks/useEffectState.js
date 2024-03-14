import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useState, useCallback, useEffect } from 'react';
/**
 * Trigger a callback on state change
 */
export default function useEffectState() {
  var _useState = useState({
      id: 0,
      callback: null
    }),
    _useState2 = _slicedToArray(_useState, 2),
    effectId = _useState2[0],
    setEffectId = _useState2[1];
  var update = useCallback(function (callback) {
    setEffectId(function (_ref) {
      var id = _ref.id;
      return {
        id: id + 1,
        callback: callback
      };
    });
  }, []);
  useEffect(function () {
    var _effectId$callback;
    (_effectId$callback = effectId.callback) === null || _effectId$callback === void 0 || _effectId$callback.call(effectId);
  }, [effectId]);
  return update;
}