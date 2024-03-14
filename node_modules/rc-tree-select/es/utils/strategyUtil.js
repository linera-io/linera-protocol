import { isCheckDisabled } from "./valueUtil";
export var SHOW_ALL = 'SHOW_ALL';
export var SHOW_PARENT = 'SHOW_PARENT';
export var SHOW_CHILD = 'SHOW_CHILD';
export function formatStrategyValues(values, strategy, keyEntities, fieldNames) {
  var valueSet = new Set(values);
  if (strategy === SHOW_CHILD) {
    return values.filter(function (key) {
      var entity = keyEntities[key];
      if (entity && entity.children && entity.children.some(function (_ref) {
        var node = _ref.node;
        return valueSet.has(node[fieldNames.value]);
      }) && entity.children.every(function (_ref2) {
        var node = _ref2.node;
        return isCheckDisabled(node) || valueSet.has(node[fieldNames.value]);
      })) {
        return false;
      }
      return true;
    });
  }
  if (strategy === SHOW_PARENT) {
    return values.filter(function (key) {
      var entity = keyEntities[key];
      var parent = entity ? entity.parent : null;
      if (parent && !isCheckDisabled(parent.node) && valueSet.has(parent.key)) {
        return false;
      }
      return true;
    });
  }
  return values;
}