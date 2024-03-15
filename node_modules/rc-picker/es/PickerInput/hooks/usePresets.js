import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import warning from "rc-util/es/warning";
export default function usePresets(presets, legacyRanges) {
  return React.useMemo(function () {
    if (presets) {
      return presets;
    }
    if (legacyRanges) {
      warning(false, '`ranges` is deprecated. Please use `presets` instead.');
      return Object.entries(legacyRanges).map(function (_ref) {
        var _ref2 = _slicedToArray(_ref, 2),
          label = _ref2[0],
          value = _ref2[1];
        return {
          label: label,
          value: value
        };
      });
    }
    return [];
  }, [presets, legacyRanges]);
}