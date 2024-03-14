import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
/**
 * Cache `value` related LabeledValue & options.
 */
export default (function (labeledValues, valueOptions) {
  var cacheRef = React.useRef({
    values: new Map(),
    options: new Map()
  });
  var filledLabeledValues = React.useMemo(function () {
    var _cacheRef$current = cacheRef.current,
      prevValueCache = _cacheRef$current.values,
      prevOptionCache = _cacheRef$current.options;

    // Fill label by cache
    var patchedValues = labeledValues.map(function (item) {
      if (item.label === undefined) {
        var _prevValueCache$get;
        return _objectSpread(_objectSpread({}, item), {}, {
          label: (_prevValueCache$get = prevValueCache.get(item.value)) === null || _prevValueCache$get === void 0 ? void 0 : _prevValueCache$get.label
        });
      }
      return item;
    });

    // Refresh cache
    var valueCache = new Map();
    var optionCache = new Map();
    patchedValues.forEach(function (item) {
      valueCache.set(item.value, item);
      optionCache.set(item.value, valueOptions.get(item.value) || prevOptionCache.get(item.value));
    });
    cacheRef.current.values = valueCache;
    cacheRef.current.options = optionCache;
    return patchedValues;
  }, [labeledValues, valueOptions]);
  var getOption = React.useCallback(function (val) {
    return valueOptions.get(val) || cacheRef.current.options.get(val);
  }, [valueOptions]);
  return [filledLabeledValues, getOption];
});