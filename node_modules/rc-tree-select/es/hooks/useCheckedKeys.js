import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import * as React from 'react';
import { conductCheck } from "rc-tree/es/utils/conductUtil";
export default (function (rawLabeledValues, rawHalfCheckedValues, treeConduction, keyEntities) {
  return React.useMemo(function () {
    var checkedKeys = rawLabeledValues.map(function (_ref) {
      var value = _ref.value;
      return value;
    });
    var halfCheckedKeys = rawHalfCheckedValues.map(function (_ref2) {
      var value = _ref2.value;
      return value;
    });
    var missingValues = checkedKeys.filter(function (key) {
      return !keyEntities[key];
    });
    if (treeConduction) {
      var _conductCheck = conductCheck(checkedKeys, true, keyEntities);
      checkedKeys = _conductCheck.checkedKeys;
      halfCheckedKeys = _conductCheck.halfCheckedKeys;
    }
    return [
    // Checked keys should fill with missing keys which should de-duplicated
    Array.from(new Set([].concat(_toConsumableArray(missingValues), _toConsumableArray(checkedKeys)))),
    // Half checked keys
    halfCheckedKeys];
  }, [rawLabeledValues, rawHalfCheckedValues, treeConduction, keyEntities]);
});