import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import * as React from 'react';
export var SEARCH_MARK = '__rc_cascader_search_mark__';
var defaultFilter = function defaultFilter(search, options, _ref) {
  var label = _ref.label;
  return options.some(function (opt) {
    return String(opt[label]).toLowerCase().includes(search.toLowerCase());
  });
};
var defaultRender = function defaultRender(inputValue, path, prefixCls, fieldNames) {
  return path.map(function (opt) {
    return opt[fieldNames.label];
  }).join(' / ');
};
export default (function (search, options, fieldNames, prefixCls, config, changeOnSelect) {
  var _config$filter = config.filter,
    filter = _config$filter === void 0 ? defaultFilter : _config$filter,
    _config$render = config.render,
    render = _config$render === void 0 ? defaultRender : _config$render,
    _config$limit = config.limit,
    limit = _config$limit === void 0 ? 50 : _config$limit,
    sort = config.sort;
  return React.useMemo(function () {
    var filteredOptions = [];
    if (!search) {
      return [];
    }
    function dig(list, pathOptions) {
      var parentDisabled = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
      list.forEach(function (option) {
        // Perf saving when `sort` is disabled and `limit` is provided
        if (!sort && limit !== false && limit > 0 && filteredOptions.length >= limit) {
          return;
        }
        var connectedPathOptions = [].concat(_toConsumableArray(pathOptions), [option]);
        var children = option[fieldNames.children];
        var mergedDisabled = parentDisabled || option.disabled;

        // If current option is filterable
        if (
        // If is leaf option
        !children || children.length === 0 ||
        // If is changeOnSelect
        changeOnSelect) {
          if (filter(search, connectedPathOptions, {
            label: fieldNames.label
          })) {
            var _objectSpread2;
            filteredOptions.push(_objectSpread(_objectSpread({}, option), {}, (_objectSpread2 = {
              disabled: mergedDisabled
            }, _defineProperty(_objectSpread2, fieldNames.label, render(search, connectedPathOptions, prefixCls, fieldNames)), _defineProperty(_objectSpread2, SEARCH_MARK, connectedPathOptions), _defineProperty(_objectSpread2, fieldNames.children, undefined), _objectSpread2)));
          }
        }
        if (children) {
          dig(option[fieldNames.children], connectedPathOptions, mergedDisabled);
        }
      });
    }
    dig(options, []);

    // Do sort
    if (sort) {
      filteredOptions.sort(function (a, b) {
        return sort(a[SEARCH_MARK], b[SEARCH_MARK], search, fieldNames);
      });
    }
    return limit !== false && limit > 0 ? filteredOptions.slice(0, limit) : filteredOptions;
  }, [search, options, fieldNames, prefixCls, render, changeOnSelect, filter, sort, limit]);
});