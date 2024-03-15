"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.SHOW_PARENT = exports.SHOW_CHILD = exports.SHOW_ALL = void 0;
exports.formatStrategyValues = formatStrategyValues;
var _valueUtil = require("./valueUtil");
var SHOW_ALL = exports.SHOW_ALL = 'SHOW_ALL';
var SHOW_PARENT = exports.SHOW_PARENT = 'SHOW_PARENT';
var SHOW_CHILD = exports.SHOW_CHILD = 'SHOW_CHILD';
function formatStrategyValues(values, strategy, keyEntities, fieldNames) {
  var valueSet = new Set(values);
  if (strategy === SHOW_CHILD) {
    return values.filter(function (key) {
      var entity = keyEntities[key];
      if (entity && entity.children && entity.children.some(function (_ref) {
        var node = _ref.node;
        return valueSet.has(node[fieldNames.value]);
      }) && entity.children.every(function (_ref2) {
        var node = _ref2.node;
        return (0, _valueUtil.isCheckDisabled)(node) || valueSet.has(node[fieldNames.value]);
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
      if (parent && !(0, _valueUtil.isCheckDisabled)(parent.node) && valueSet.has(parent.key)) {
        return false;
      }
      return true;
    });
  }
  return values;
}