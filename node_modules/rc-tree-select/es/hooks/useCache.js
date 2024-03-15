import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
/**
 * This function will try to call requestIdleCallback if available to save performance.
 * No need `getLabel` here since already fetch on `rawLabeledValue`.
 */
export default (function (values) {
  var cacheRef = React.useRef({
    valueLabels: new Map()
  });
  return React.useMemo(function () {
    var valueLabels = cacheRef.current.valueLabels;
    var valueLabelsCache = new Map();
    var filledValues = values.map(function (item) {
      var _item$label;
      var value = item.value;
      var mergedLabel = (_item$label = item.label) !== null && _item$label !== void 0 ? _item$label : valueLabels.get(value);

      // Save in cache
      valueLabelsCache.set(value, mergedLabel);
      return _objectSpread(_objectSpread({}, item), {}, {
        label: mergedLabel
      });
    });
    cacheRef.current.valueLabels = valueLabelsCache;
    return [filledValues];
  }, [values]);
});