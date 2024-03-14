import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useEffect, useState } from 'react';
import { generateColor } from "../util";
function hasValue(value) {
  return value !== undefined;
}
var useColorState = function useColorState(defaultStateValue, option) {
  var defaultValue = option.defaultValue,
    value = option.value;
  var _useState = useState(function () {
      var mergeState;
      if (hasValue(value)) {
        mergeState = value;
      } else if (hasValue(defaultValue)) {
        mergeState = defaultValue;
      } else {
        mergeState = defaultStateValue;
      }
      return generateColor(mergeState);
    }),
    _useState2 = _slicedToArray(_useState, 2),
    colorValue = _useState2[0],
    setColorValue = _useState2[1];
  useEffect(function () {
    if (value) {
      setColorValue(generateColor(value));
    }
  }, [value]);
  return [colorValue, setColorValue];
};
export default useColorState;