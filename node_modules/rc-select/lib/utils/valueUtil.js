"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.fillFieldNames = fillFieldNames;
exports.flattenOptions = flattenOptions;
exports.getSeparatedContent = void 0;
exports.injectPropsWithOption = injectPropsWithOption;
exports.isValidCount = isValidCount;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _toArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toArray"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
function getKey(data, index) {
  var key = data.key;
  var value;
  if ('value' in data) {
    value = data.value;
  }
  if (key !== null && key !== undefined) {
    return key;
  }
  if (value !== undefined) {
    return value;
  }
  return "rc-index-key-".concat(index);
}
function isValidCount(value) {
  return typeof value !== 'undefined' && !Number.isNaN(value);
}
function fillFieldNames(fieldNames, childrenAsData) {
  var _ref = fieldNames || {},
    label = _ref.label,
    value = _ref.value,
    options = _ref.options,
    groupLabel = _ref.groupLabel;
  var mergedLabel = label || (childrenAsData ? 'children' : 'label');
  return {
    label: mergedLabel,
    value: value || 'value',
    options: options || 'options',
    groupLabel: groupLabel || mergedLabel
  };
}

/**
 * Flat options into flatten list.
 * We use `optionOnly` here is aim to avoid user use nested option group.
 * Here is simply set `key` to the index if not provided.
 */
function flattenOptions(options) {
  var _ref2 = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
    fieldNames = _ref2.fieldNames,
    childrenAsData = _ref2.childrenAsData;
  var flattenList = [];
  var _fillFieldNames = fillFieldNames(fieldNames, false),
    fieldLabel = _fillFieldNames.label,
    fieldValue = _fillFieldNames.value,
    fieldOptions = _fillFieldNames.options,
    groupLabel = _fillFieldNames.groupLabel;
  function dig(list, isGroupOption) {
    if (!Array.isArray(list)) {
      return;
    }
    list.forEach(function (data) {
      if (isGroupOption || !(fieldOptions in data)) {
        var value = data[fieldValue];

        // Option
        flattenList.push({
          key: getKey(data, flattenList.length),
          groupOption: isGroupOption,
          data: data,
          label: data[fieldLabel],
          value: value
        });
      } else {
        var grpLabel = data[groupLabel];
        if (grpLabel === undefined && childrenAsData) {
          grpLabel = data.label;
        }

        // Option Group
        flattenList.push({
          key: getKey(data, flattenList.length),
          group: true,
          data: data,
          label: grpLabel
        });
        dig(data[fieldOptions], true);
      }
    });
  }
  dig(options, false);
  return flattenList;
}

/**
 * Inject `props` into `option` for legacy usage
 */
function injectPropsWithOption(option) {
  var newOption = (0, _objectSpread2.default)({}, option);
  if (!('props' in newOption)) {
    Object.defineProperty(newOption, 'props', {
      get: function get() {
        (0, _warning.default)(false, 'Return type is option instead of Option instance. Please read value directly instead of reading from `props`.');
        return newOption;
      }
    });
  }
  return newOption;
}
var getSeparatedContent = exports.getSeparatedContent = function getSeparatedContent(text, tokens, end) {
  if (!tokens || !tokens.length) {
    return null;
  }
  var match = false;
  var separate = function separate(str, _ref3) {
    var _ref4 = (0, _toArray2.default)(_ref3),
      token = _ref4[0],
      restTokens = _ref4.slice(1);
    if (!token) {
      return [str];
    }
    var list = str.split(token);
    match = match || list.length > 1;
    return list.reduce(function (prevList, unitStr) {
      return [].concat((0, _toConsumableArray2.default)(prevList), (0, _toConsumableArray2.default)(separate(unitStr, restTokens)));
    }, []).filter(Boolean);
  };
  var list = separate(text, tokens);
  if (match) {
    return typeof end !== 'undefined' ? list.slice(0, end) : list;
  } else {
    return null;
  }
};