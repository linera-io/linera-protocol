"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useSelect;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _conductUtil = require("rc-tree/lib/utils/conductUtil");
var _commonUtil = require("../utils/commonUtil");
var _treeUtil = require("../utils/treeUtil");
function useSelect(multiple, triggerChange, checkedValues, halfCheckedValues, missingCheckedValues, getPathKeyEntities, getValueByKeyPath, showCheckedStrategy) {
  return function (valuePath) {
    if (!multiple) {
      triggerChange(valuePath);
    } else {
      // Prepare conduct required info
      var pathKey = (0, _commonUtil.toPathKey)(valuePath);
      var checkedPathKeys = (0, _commonUtil.toPathKeys)(checkedValues);
      var halfCheckedPathKeys = (0, _commonUtil.toPathKeys)(halfCheckedValues);
      var existInChecked = checkedPathKeys.includes(pathKey);
      var existInMissing = missingCheckedValues.some(function (valueCells) {
        return (0, _commonUtil.toPathKey)(valueCells) === pathKey;
      });

      // Do update
      var nextCheckedValues = checkedValues;
      var nextMissingValues = missingCheckedValues;
      if (existInMissing && !existInChecked) {
        // Missing value only do filter
        nextMissingValues = missingCheckedValues.filter(function (valueCells) {
          return (0, _commonUtil.toPathKey)(valueCells) !== pathKey;
        });
      } else {
        // Update checked key first
        var nextRawCheckedKeys = existInChecked ? checkedPathKeys.filter(function (key) {
          return key !== pathKey;
        }) : [].concat((0, _toConsumableArray2.default)(checkedPathKeys), [pathKey]);
        var pathKeyEntities = getPathKeyEntities();

        // Conduction by selected or not
        var checkedKeys;
        if (existInChecked) {
          var _conductCheck = (0, _conductUtil.conductCheck)(nextRawCheckedKeys, {
            checked: false,
            halfCheckedKeys: halfCheckedPathKeys
          }, pathKeyEntities);
          checkedKeys = _conductCheck.checkedKeys;
        } else {
          var _conductCheck2 = (0, _conductUtil.conductCheck)(nextRawCheckedKeys, true, pathKeyEntities);
          checkedKeys = _conductCheck2.checkedKeys;
        }

        // Roll up to parent level keys
        var deDuplicatedKeys = (0, _treeUtil.formatStrategyValues)(checkedKeys, getPathKeyEntities, showCheckedStrategy);
        nextCheckedValues = getValueByKeyPath(deDuplicatedKeys);
      }
      triggerChange([].concat((0, _toConsumableArray2.default)(nextMissingValues), (0, _toConsumableArray2.default)(nextCheckedValues)));
    }
  };
}