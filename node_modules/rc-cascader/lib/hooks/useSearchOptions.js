"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.SEARCH_MARK = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread3 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var React = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var SEARCH_MARK = exports.SEARCH_MARK = '__rc_cascader_search_mark__';
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
var _default = exports.default = function _default(search, options, fieldNames, prefixCls, config, changeOnSelect) {
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
        var connectedPathOptions = [].concat((0, _toConsumableArray2.default)(pathOptions), [option]);
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
            filteredOptions.push((0, _objectSpread3.default)((0, _objectSpread3.default)({}, option), {}, (_objectSpread2 = {
              disabled: mergedDisabled
            }, (0, _defineProperty2.default)(_objectSpread2, fieldNames.label, render(search, connectedPathOptions, prefixCls, fieldNames)), (0, _defineProperty2.default)(_objectSpread2, SEARCH_MARK, connectedPathOptions), (0, _defineProperty2.default)(_objectSpread2, fieldNames.children, undefined), _objectSpread2)));
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
};