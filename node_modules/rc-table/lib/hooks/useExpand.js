"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useExpand;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var React = _interopRequireWildcard(require("react"));
var _constant = require("../constant");
var _expandUtil = require("../utils/expandUtil");
var _legacyUtil = require("../utils/legacyUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function useExpand(props, mergedData, getRowKey) {
  var expandableConfig = (0, _legacyUtil.getExpandableProps)(props);
  var expandIcon = expandableConfig.expandIcon,
    expandedRowKeys = expandableConfig.expandedRowKeys,
    defaultExpandedRowKeys = expandableConfig.defaultExpandedRowKeys,
    defaultExpandAllRows = expandableConfig.defaultExpandAllRows,
    expandedRowRender = expandableConfig.expandedRowRender,
    onExpand = expandableConfig.onExpand,
    onExpandedRowsChange = expandableConfig.onExpandedRowsChange,
    childrenColumnName = expandableConfig.childrenColumnName;
  var mergedExpandIcon = expandIcon || _expandUtil.renderExpandIcon;
  var mergedChildrenColumnName = childrenColumnName || 'children';
  var expandableType = React.useMemo(function () {
    if (expandedRowRender) {
      return 'row';
    }
    /* eslint-disable no-underscore-dangle */
    /**
     * Fix https://github.com/ant-design/ant-design/issues/21154
     * This is a workaround to not to break current behavior.
     * We can remove follow code after final release.
     *
     * To other developer:
     *  Do not use `__PARENT_RENDER_ICON__` in prod since we will remove this when refactor
     */
    if (props.expandable && props.internalHooks === _constant.INTERNAL_HOOKS && props.expandable.__PARENT_RENDER_ICON__ || mergedData.some(function (record) {
      return record && (0, _typeof2.default)(record) === 'object' && record[mergedChildrenColumnName];
    })) {
      return 'nest';
    }
    /* eslint-enable */
    return false;
  }, [!!expandedRowRender, mergedData]);
  var _React$useState = React.useState(function () {
      if (defaultExpandedRowKeys) {
        return defaultExpandedRowKeys;
      }
      if (defaultExpandAllRows) {
        return (0, _expandUtil.findAllChildrenKeys)(mergedData, getRowKey, mergedChildrenColumnName);
      }
      return [];
    }),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    innerExpandedKeys = _React$useState2[0],
    setInnerExpandedKeys = _React$useState2[1];
  var mergedExpandedKeys = React.useMemo(function () {
    return new Set(expandedRowKeys || innerExpandedKeys || []);
  }, [expandedRowKeys, innerExpandedKeys]);
  var onTriggerExpand = React.useCallback(function (record) {
    var key = getRowKey(record, mergedData.indexOf(record));
    var newExpandedKeys;
    var hasKey = mergedExpandedKeys.has(key);
    if (hasKey) {
      mergedExpandedKeys.delete(key);
      newExpandedKeys = (0, _toConsumableArray2.default)(mergedExpandedKeys);
    } else {
      newExpandedKeys = [].concat((0, _toConsumableArray2.default)(mergedExpandedKeys), [key]);
    }
    setInnerExpandedKeys(newExpandedKeys);
    if (onExpand) {
      onExpand(!hasKey, record);
    }
    if (onExpandedRowsChange) {
      onExpandedRowsChange(newExpandedKeys);
    }
  }, [getRowKey, mergedExpandedKeys, mergedData, onExpand, onExpandedRowsChange]);

  // Warning if use `expandedRowRender` and nest children in the same time
  if (process.env.NODE_ENV !== 'production' && expandedRowRender && mergedData.some(function (record) {
    return Array.isArray(record === null || record === void 0 ? void 0 : record[mergedChildrenColumnName]);
  })) {
    (0, _warning.default)(false, '`expandedRowRender` should not use with nested Table');
  }
  return [expandableConfig, expandableType, mergedExpandedKeys, mergedExpandIcon, mergedChildrenColumnName, onTriggerExpand];
}