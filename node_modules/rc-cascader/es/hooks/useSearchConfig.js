import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import warning from "rc-util/es/warning";
import * as React from 'react';
// Convert `showSearch` to unique config
export default function useSearchConfig(showSearch) {
  return React.useMemo(function () {
    if (!showSearch) {
      return [false, {}];
    }
    var searchConfig = {
      matchInputWidth: true,
      limit: 50
    };
    if (showSearch && _typeof(showSearch) === 'object') {
      searchConfig = _objectSpread(_objectSpread({}, searchConfig), showSearch);
    }
    if (searchConfig.limit <= 0) {
      delete searchConfig.limit;
      if (process.env.NODE_ENV !== 'production') {
        warning(false, "'limit' of showSearch should be positive number or false.");
      }
    }
    return [true, searchConfig];
  }, [showSearch]);
}