export function toArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  return value !== undefined ? [value] : [];
}
export function fillFieldNames(fieldNames) {
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
export function isCheckDisabled(node) {
  return !node || node.disabled || node.disableCheckbox || node.checkable === false;
}

/** Loop fetch all the keys exist in the tree */
export function getAllKeys(treeData, fieldNames) {
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
export function isNil(val) {
  return val === null || val === undefined;
}