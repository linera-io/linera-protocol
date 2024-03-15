var INTERNAL_KEY_PREFIX = 'RC_TABLE_KEY';
function toArray(arr) {
  if (arr === undefined || arr === null) {
    return [];
  }
  return Array.isArray(arr) ? arr : [arr];
}
export function getColumnsKey(columns) {
  var columnKeys = [];
  var keys = {};
  columns.forEach(function (column) {
    var _ref = column || {},
      key = _ref.key,
      dataIndex = _ref.dataIndex;
    var mergedKey = key || toArray(dataIndex).join('-') || INTERNAL_KEY_PREFIX;
    while (keys[mergedKey]) {
      mergedKey = "".concat(mergedKey, "_next");
    }
    keys[mergedKey] = true;
    columnKeys.push(mergedKey);
  });
  return columnKeys;
}
export function validateValue(val) {
  return val !== null && val !== undefined;
}