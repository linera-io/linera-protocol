"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VALUE_SPLIT = exports.SHOW_PARENT = exports.SHOW_CHILD = void 0;
exports.fillFieldNames = fillFieldNames;
exports.getFullPathKeys = getFullPathKeys;
exports.isLeaf = isLeaf;
exports.scrollIntoParentView = scrollIntoParentView;
exports.toPathKey = toPathKey;
exports.toPathKeys = toPathKeys;
exports.toPathValueStr = toPathValueStr;
exports.toRawValues = toRawValues;
var _useSearchOptions = require("../hooks/useSearchOptions");
var VALUE_SPLIT = exports.VALUE_SPLIT = '__RC_CASCADER_SPLIT__';
var SHOW_PARENT = exports.SHOW_PARENT = 'SHOW_PARENT';
var SHOW_CHILD = exports.SHOW_CHILD = 'SHOW_CHILD';

/**
 * Will convert value to string, and join with `VALUE_SPLIT`
 */
function toPathKey(value) {
  return value.join(VALUE_SPLIT);
}

/**
 * Batch convert value to string, and join with `VALUE_SPLIT`
 */
function toPathKeys(value) {
  return value.map(toPathKey);
}
function toPathValueStr(pathKey) {
  return pathKey.split(VALUE_SPLIT);
}
function fillFieldNames(fieldNames) {
  var _ref = fieldNames || {},
    label = _ref.label,
    value = _ref.value,
    children = _ref.children;
  var val = value || 'value';
  return {
    label: label || 'label',
    value: val,
    key: val,
    children: children || 'children'
  };
}
function isLeaf(option, fieldNames) {
  var _option$isLeaf, _option$fieldNames$ch;
  return (_option$isLeaf = option.isLeaf) !== null && _option$isLeaf !== void 0 ? _option$isLeaf : !((_option$fieldNames$ch = option[fieldNames.children]) !== null && _option$fieldNames$ch !== void 0 && _option$fieldNames$ch.length);
}
function scrollIntoParentView(element) {
  var parent = element.parentElement;
  if (!parent) {
    return;
  }
  var elementToParent = element.offsetTop - parent.offsetTop; // offsetParent may not be parent.
  if (elementToParent - parent.scrollTop < 0) {
    parent.scrollTo({
      top: elementToParent
    });
  } else if (elementToParent + element.offsetHeight - parent.scrollTop > parent.offsetHeight) {
    parent.scrollTo({
      top: elementToParent + element.offsetHeight - parent.offsetHeight
    });
  }
}
function getFullPathKeys(options, fieldNames) {
  return options.map(function (item) {
    var _item$SEARCH_MARK;
    return (_item$SEARCH_MARK = item[_useSearchOptions.SEARCH_MARK]) === null || _item$SEARCH_MARK === void 0 ? void 0 : _item$SEARCH_MARK.map(function (opt) {
      return opt[fieldNames.value];
    });
  });
}
function isMultipleValue(value) {
  return Array.isArray(value) && Array.isArray(value[0]);
}
function toRawValues(value) {
  if (!value) {
    return [];
  }
  if (isMultipleValue(value)) {
    return value;
  }
  return (value.length === 0 ? [] : [value]).map(function (val) {
    return Array.isArray(val) ? val : [val];
  });
}