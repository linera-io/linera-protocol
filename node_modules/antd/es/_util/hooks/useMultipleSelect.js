import { useCallback, useState } from 'react';
/**
 * @title multipleSelect hooks
 * @description multipleSelect by hold down shift key
 */
export default function useMultipleSelect(getKey) {
  const [prevSelectedIndex, setPrevSelectedIndex] = useState(null);
  const multipleSelect = useCallback((currentSelectedIndex, data, selectedKeys) => {
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