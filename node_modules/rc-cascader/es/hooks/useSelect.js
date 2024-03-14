import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import { conductCheck } from "rc-tree/es/utils/conductUtil";
import { toPathKey, toPathKeys } from "../utils/commonUtil";
import { formatStrategyValues } from "../utils/treeUtil";
export default function useSelect(multiple, triggerChange, checkedValues, halfCheckedValues, missingCheckedValues, getPathKeyEntities, getValueByKeyPath, showCheckedStrategy) {
  return function (valuePath) {
    if (!multiple) {
      triggerChange(valuePath);
    } else {
      // Prepare conduct required info
      var pathKey = toPathKey(valuePath);
      var checkedPathKeys = toPathKeys(checkedValues);
      var halfCheckedPathKeys = toPathKeys(halfCheckedValues);
      var existInChecked = checkedPathKeys.includes(pathKey);
      var existInMissing = missingCheckedValues.some(function (valueCells) {
        return toPathKey(valueCells) === pathKey;
      });

      // Do update
      var nextCheckedValues = checkedValues;
      var nextMissingValues = missingCheckedValues;
      if (existInMissing && !existInChecked) {
        // Missing value only do filter
        nextMissingValues = missingCheckedValues.filter(function (valueCells) {
          return toPathKey(valueCells) !== pathKey;
        });
      } else {
        // Update checked key first
        var nextRawCheckedKeys = existInChecked ? checkedPathKeys.filter(function (key) {
          return key !== pathKey;
        }) : [].concat(_toConsumableArray(checkedPathKeys), [pathKey]);
        var pathKeyEntities = getPathKeyEntities();

        // Conduction by selected or not
        var checkedKeys;
        if (existInChecked) {
          var _conductCheck = conductCheck(nextRawCheckedKeys, {
            checked: false,
            halfCheckedKeys: halfCheckedPathKeys
          }, pathKeyEntities);
          checkedKeys = _conductCheck.checkedKeys;
        } else {
          var _conductCheck2 = conductCheck(nextRawCheckedKeys, true, pathKeyEntities);
          checkedKeys = _conductCheck2.checkedKeys;
        }

        // Roll up to parent level keys
        var deDuplicatedKeys = formatStrategyValues(checkedKeys, getPathKeyEntities, showCheckedStrategy);
        nextCheckedValues = getValueByKeyPath(deDuplicatedKeys);
      }
      triggerChange([].concat(_toConsumableArray(nextMissingValues), _toConsumableArray(nextCheckedValues)));
    }
  };
}