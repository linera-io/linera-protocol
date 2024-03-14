"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread3 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var React = _interopRequireWildcard(require("react"));
var _legacyUtil = require("../utils/legacyUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var _default = exports.default = function _default(treeData, searchValue, _ref) {
  var treeNodeFilterProp = _ref.treeNodeFilterProp,
    filterTreeNode = _ref.filterTreeNode,
    fieldNames = _ref.fieldNames;
  var fieldChildren = fieldNames.children;
  return React.useMemo(function () {
    if (!searchValue || filterTreeNode === false) {
      return treeData;
    }
    var filterOptionFunc;
    if (typeof filterTreeNode === 'function') {
      filterOptionFunc = filterTreeNode;
    } else {
      var upperStr = searchValue.toUpperCase();
      filterOptionFunc = function filterOptionFunc(_, dataNode) {
        var value = dataNode[treeNodeFilterProp];
        return String(value).toUpperCase().includes(upperStr);
      };
    }
    function dig(list) {
      var keepAll = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
      return list.reduce(function (total, dataNode) {
        var children = dataNode[fieldChildren];
        var match = keepAll || filterOptionFunc(searchValue, (0, _legacyUtil.fillLegacyProps)(dataNode));
        var childList = dig(children || [], match);
        if (match || childList.length) {
          total.push((0, _objectSpread3.default)((0, _objectSpread3.default)({}, dataNode), {}, (0, _defineProperty2.default)({
            isLeaf: undefined
          }, fieldChildren, childList)));
        }
        return total;
      }, []);
    }
    return dig(treeData);
  }, [treeData, searchValue, fieldChildren, treeNodeFilterProp, filterTreeNode]);
};