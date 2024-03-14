import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import { toPathOptions } from "../utils/treeUtil";
import * as React from 'react';
import { toPathKey } from "../utils/commonUtil";
export default (function (rawValues, options, fieldNames, multiple, displayRender) {
  return React.useMemo(function () {
    var mergedDisplayRender = displayRender ||
    // Default displayRender
    function (labels) {
      var mergedLabels = multiple ? labels.slice(-1) : labels;
      var SPLIT = ' / ';
      if (mergedLabels.every(function (label) {
        return ['string', 'number'].includes(_typeof(label));
      })) {
        return mergedLabels.join(SPLIT);
      }

      // If exist non-string value, use ReactNode instead
      return mergedLabels.reduce(function (list, label, index) {
        var keyedLabel = /*#__PURE__*/React.isValidElement(label) ? /*#__PURE__*/React.cloneElement(label, {
          key: index
        }) : label;
        if (index === 0) {
          return [keyedLabel];
        }
        return [].concat(_toConsumableArray(list), [SPLIT, keyedLabel]);
      }, []);
    };
    return rawValues.map(function (valueCells) {
      var _valueOptions;
      var valueOptions = toPathOptions(valueCells, options, fieldNames);
      var label = mergedDisplayRender(valueOptions.map(function (_ref) {
        var _option$fieldNames$la;
        var option = _ref.option,
          value = _ref.value;
        return (_option$fieldNames$la = option === null || option === void 0 ? void 0 : option[fieldNames.label]) !== null && _option$fieldNames$la !== void 0 ? _option$fieldNames$la : value;
      }), valueOptions.map(function (_ref2) {
        var option = _ref2.option;
        return option;
      }));
      var value = toPathKey(valueCells);
      return {
        label: label,
        value: value,
        key: value,
        valueCells: valueCells,
        disabled: (_valueOptions = valueOptions[valueOptions.length - 1]) === null || _valueOptions === void 0 || (_valueOptions = _valueOptions.option) === null || _valueOptions === void 0 ? void 0 : _valueOptions.disabled
      };
    });
  }, [rawValues, options, fieldNames, displayRender, multiple]);
});