import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { conductCheck } from "rc-tree/es/utils/conductUtil";
import * as React from 'react';
import { toPathKeys } from "../utils/commonUtil";
export default function useValues(multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues) {
  // Fill `rawValues` with checked conduction values
  return React.useMemo(function () {
    var _getMissingValues = getMissingValues(rawValues),
      _getMissingValues2 = _slicedToArray(_getMissingValues, 2),
      existValues = _getMissingValues2[0],
      missingValues = _getMissingValues2[1];
    if (!multiple || !rawValues.length) {
      return [existValues, [], missingValues];
    }
    var keyPathValues = toPathKeys(existValues);
    var keyPathEntities = getPathKeyEntities();
    var _conductCheck = conductCheck(keyPathValues, true, keyPathEntities),
      checkedKeys = _conductCheck.checkedKeys,
      halfCheckedKeys = _conductCheck.halfCheckedKeys;

    // Convert key back to value cells
    return [getValueByKeyPath(checkedKeys), getValueByKeyPath(halfCheckedKeys), missingValues];
  }, [multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues]);
}