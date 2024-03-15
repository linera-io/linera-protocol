"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getColumnKey = getColumnKey;
exports.getColumnPos = getColumnPos;
exports.renderColumnTitle = renderColumnTitle;
exports.safeColumnTitle = safeColumnTitle;
function getColumnKey(column, defaultKey) {
  if ('key' in column && column.key !== undefined && column.key !== null) {
    return column.key;
  }
  if (column.dataIndex) {
    return Array.isArray(column.dataIndex) ? column.dataIndex.join('.') : column.dataIndex;
  }
  return defaultKey;
}
function getColumnPos(index, pos) {
  return pos ? `${pos}-${index}` : `${index}`;
}
function renderColumnTitle(title, props) {
  if (typeof title === 'function') {
    return title(props);
  }
  return title;
}
/**
 * Safe get column title
 *
 * Should filter [object Object]
 *
 * @param title
 * @returns
 */
function safeColumnTitle(title, props) {
  const res = renderColumnTitle(title, props);
  if (Object.prototype.toString.call(res) === '[object Object]') return '';
  return res;
}