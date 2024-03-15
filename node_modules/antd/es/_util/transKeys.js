export const groupKeysMap = keys => {
  const map = new Map();
  keys.forEach((key, index) => {
    map.set(key, index);
  });
  return map;
};
export const groupDisabledKeysMap = dataSource => {
  const map = new Map();
  dataSource.forEach((_ref, index) => {
    let {
      disabled,
      key
    } = _ref;
    if (disabled) {
      map.set(key, index);
    }
  });
  return map;
};