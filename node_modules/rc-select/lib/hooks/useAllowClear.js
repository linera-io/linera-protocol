"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useAllowClear = void 0;
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _TransBtn = _interopRequireDefault(require("../TransBtn"));
var _react = _interopRequireDefault(require("react"));
var useAllowClear = exports.useAllowClear = function useAllowClear(prefixCls, onClearMouseDown, displayValues, allowClear, clearIcon) {
  var disabled = arguments.length > 5 && arguments[5] !== undefined ? arguments[5] : false;
  var mergedSearchValue = arguments.length > 6 ? arguments[6] : undefined;
  var mode = arguments.length > 7 ? arguments[7] : undefined;
  var mergedClearIcon = _react.default.useMemo(function () {
    if ((0, _typeof2.default)(allowClear) === 'object') {
      return allowClear.clearIcon;
    }
    if (clearIcon) {
      return clearIcon;
    }
  }, [allowClear, clearIcon]);
  var mergedAllowClear = _react.default.useMemo(function () {
    if (!disabled && !!allowClear && (displayValues.length || mergedSearchValue) && !(mode === 'combobox' && mergedSearchValue === '')) {
      return true;
    }
    return false;
  }, [allowClear, disabled, displayValues.length, mergedSearchValue, mode]);
  return {
    allowClear: mergedAllowClear,
    clearIcon: /*#__PURE__*/_react.default.createElement(_TransBtn.default, {
      className: "".concat(prefixCls, "-clear"),
      onMouseDown: onClearMouseDown,
      customizeIcon: mergedClearIcon
    }, "\xD7")
  };
};