"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var React = _interopRequireWildcard(require("react"));
var _treeUtil = require("rc-tree/lib/utils/treeUtil");
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _valueUtil = require("../utils/valueUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var _default = exports.default = function _default(treeData, fieldNames) {
  return React.useMemo(function () {
    var collection = (0, _treeUtil.convertDataToEntities)(treeData, {
      fieldNames: fieldNames,
      initWrapper: function initWrapper(wrapper) {
        return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, wrapper), {}, {
          valueEntities: new Map()
        });
      },
      processEntity: function processEntity(entity, wrapper) {
        var val = entity.node[fieldNames.value];

        // Check if exist same value
        if (process.env.NODE_ENV !== 'production') {
          var key = entity.node.key;
          (0, _warning.default)(!(0, _valueUtil.isNil)(val), 'TreeNode `value` is invalidate: undefined');
          (0, _warning.default)(!wrapper.valueEntities.has(val), "Same `value` exist in the tree: ".concat(val));
          (0, _warning.default)(!key || String(key) === String(val), "`key` or `value` with TreeNode must be the same or you can remove one of them. key: ".concat(key, ", value: ").concat(val, "."));
        }
        wrapper.valueEntities.set(val, entity);
      }
    });
    return collection;
  }, [treeData, fieldNames]);
};