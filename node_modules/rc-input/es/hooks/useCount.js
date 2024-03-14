import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _typeof from "@babel/runtime/helpers/esm/typeof";
var _excluded = ["show"];
import * as React from 'react';
/**
 * Cut `value` by the `count.max` prop.
 */
export function inCountRange(value, countConfig) {
  if (!countConfig.max) {
    return true;
  }
  var count = countConfig.strategy(value);
  return count <= countConfig.max;
}
export default function useCount(count, showCount) {
  return React.useMemo(function () {
    var mergedConfig = {};
    if (showCount) {
      mergedConfig.show = _typeof(showCount) === 'object' && showCount.formatter ? showCount.formatter : !!showCount;
    }
    mergedConfig = _objectSpread(_objectSpread({}, mergedConfig), count);
    var _ref = mergedConfig,
      show = _ref.show,
      rest = _objectWithoutProperties(_ref, _excluded);
    return _objectSpread(_objectSpread({}, rest), {}, {
      show: !!show,
      showFormatter: typeof show === 'function' ? show : undefined,
      strategy: rest.strategy || function (value) {
        return value.length;
      }
    });
  }, [count, showCount]);
}