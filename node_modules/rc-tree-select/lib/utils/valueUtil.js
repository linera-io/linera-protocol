"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.fillFieldNames = fillFieldNames;
exports.getAllKeys = getAllKeys;
exports.isCheckDisabled = isCheckDisabled;
exports.isNil = isNil;
exports.toArray = toArray;
function toArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  return value !== undefined ? [value] : [];
}
function fillFieldNames(fieldNames) {
  var _ref = fieldNames || {},
    label = _ref.label,
    value = _ref.value,
    children = _ref.children;
  var mergedValue = value || 'value';
  return {
    _title: label ? [label] : ['title', 'label'],
    value: mergedValue,
    key: mergedValue,
    children: children || 'children'
  };
}
function isCheckDisabled(node) {
  return !node || node.disabled || node.disableCheckbox || node.checkable === false;
}

/** Loop fetch all the keys exist in the tree */
function getAllKeys(treeData, fieldNames) {
  var keys = [];
  function dig(list) {
    list.forEach(function (item) {
      var children = item[fieldNames.children];
      if (children) {
        keys.push(item[fieldNames.value]);
        dig(children);
      }
    });
  }
  dig(treeData);
  return keys;
}
function isNil(val) {
  return val === null || val === undefined;
}