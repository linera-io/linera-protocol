"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.groupKeysMap = exports.groupDisabledKeysMap = void 0;
const groupKeysMap = keys => {
  const map = new Map();
  keys.forEach((key, index) => {
    map.set(key, index);
  });
  return map;
};
exports.groupKeysMap = groupKeysMap;
const groupDisabledKeysMap = dataSource => {
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
exports.groupDisabledKeysMap = groupDisabledKeysMap;