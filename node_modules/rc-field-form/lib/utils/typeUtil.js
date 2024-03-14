"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isFormInstance = isFormInstance;
exports.toArray = toArray;
function toArray(value) {
  if (value === undefined || value === null) {
    return [];
  }
  return Array.isArray(value) ? value : [value];
}
function isFormInstance(form) {
  return form && !!form._init;
}