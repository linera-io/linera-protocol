import _typeof from "@babel/runtime/helpers/esm/typeof";
import TransBtn from "../TransBtn";
import React from 'react';
export var useAllowClear = function useAllowClear(prefixCls, onClearMouseDown, displayValues, allowClear, clearIcon) {
  var disabled = arguments.length > 5 && arguments[5] !== undefined ? arguments[5] : false;
  var mergedSearchValue = arguments.length > 6 ? arguments[6] : undefined;
  var mode = arguments.length > 7 ? arguments[7] : undefined;
  var mergedClearIcon = React.useMemo(function () {
    if (_typeof(allowClear) === 'object') {
      return allowClear.clearIcon;
    }
    if (clearIcon) {
      return clearIcon;
    }
  }, [allowClear, clearIcon]);
  var mergedAllowClear = React.useMemo(function () {
    if (!disabled && !!allowClear && (displayValues.length || mergedSearchValue) && !(mode === 'combobox' && mergedSearchValue === '')) {
      return true;
    }
    return false;
  }, [allowClear, disabled, displayValues.length, mergedSearchValue, mode]);
  return {
    allowClear: mergedAllowClear,
    clearIcon: /*#__PURE__*/React.createElement(TransBtn, {
      className: "".concat(prefixCls, "-clear"),
      onMouseDown: onClearMouseDown,
      customizeIcon: mergedClearIcon
    }, "\xD7")
  };
};