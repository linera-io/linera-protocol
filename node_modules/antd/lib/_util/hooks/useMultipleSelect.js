"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useMultipleSelect;
var _react = require("react");
/**
 * @title multipleSelect hooks
 * @description multipleSelect by hold down shift key
 */
function useMultipleSelect(getKey) {
  const [prevSelectedIndex, setPrevSelectedIndex] = (0, _react.useState)(null);
  const multipleSelect = (0, _react.useCallback)((currentSelectedIndex, data, selectedKeys) => {
    const configPrevSelectedIndex = prevSelectedIndex !== null && prevSelectedIndex !== void 0 ? prevSelectedIndex : currentSelectedIndex;
    // add/delete the selected range
    const startIndex = Math.min(configPrevSelectedIndex || 0, currentSelectedIndex);
    const endIndex = Math.max(configPrevSelectedIndex || 0, currentSelectedIndex);
    const rangeKeys = data.slice(startIndex, endIndex + 1).map(item => getKey(item));
    const shouldSelected = rangeKeys.some(rangeKey => !selectedKeys.has(rangeKey));
    const changedKeys = [];
    rangeKeys.forEach(item => {
      if (shouldSelected) {
        if (!selectedKeys.has(item)) {
          changedKeys.push(item);
        }
        selectedKeys.add(item);
      } else {
        selectedKeys.delete(item);
        changedKeys.push(item);
      }
    });
    setPrevSelectedIndex(shouldSelected ? endIndex : null);
    return changedKeys;
  }, [prevSelectedIndex]);
  const updatePrevSelectedIndex = val => {
    setPrevSelectedIndex(val);
  };
  return [multipleSelect, updatePrevSelectedIndex];
}