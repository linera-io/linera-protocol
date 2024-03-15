"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useValues;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _conductUtil = require("rc-tree/lib/utils/conductUtil");
var React = _interopRequireWildcard(require("react"));
var _commonUtil = require("../utils/commonUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function useValues(multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues) {
  // Fill `rawValues` with checked conduction values
  return React.useMemo(function () {
    var _getMissingValues = getMissingValues(rawValues),
      _getMissingValues2 = (0, _slicedToArray2.default)(_getMissingValues, 2),
      existValues = _getMissingValues2[0],
      missingValues = _getMissingValues2[1];
    if (!multiple || !rawValues.length) {
      return [existValues, [], missingValues];
    }
    var keyPathValues = (0, _commonUtil.toPathKeys)(existValues);
    var keyPathEntities = getPathKeyEntities();
    var _conductCheck = (0, _conductUtil.conductCheck)(keyPathValues, true, keyPathEntities),
      checkedKeys = _conductCheck.checkedKeys,
      halfCheckedKeys = _conductCheck.halfCheckedKeys;

    // Convert key back to value cells
    return [getValueByKeyPath(checkedKeys), getValueByKeyPath(halfCheckedKeys), missingValues];
  }, [multiple, rawValues, getPathKeyEntities, getValueByKeyPath, getMissingValues]);
}