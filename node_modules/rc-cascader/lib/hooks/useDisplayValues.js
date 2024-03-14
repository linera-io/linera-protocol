"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _treeUtil = require("../utils/treeUtil");
var React = _interopRequireWildcard(require("react"));
var _commonUtil = require("../utils/commonUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var _default = exports.default = function _default(rawValues, options, fieldNames, multiple, displayRender) {
  return React.useMemo(function () {
    var mergedDisplayRender = displayRender ||
    // Default displayRender
    function (labels) {
      var mergedLabels = multiple ? labels.slice(-1) : labels;
      var SPLIT = ' / ';
      if (mergedLabels.every(function (label) {
        return ['string', 'number'].includes((0, _typeof2.default)(label));
      })) {
        return mergedLabels.join(SPLIT);
      }

      // If exist non-string value, use ReactNode instead
      return mergedLabels.reduce(function (list, label, index) {
        var keyedLabel = /*#__PURE__*/React.isValidElement(label) ? /*#__PURE__*/React.cloneElement(label, {
          key: index
        }) : label;
        if (index === 0) {
          return [keyedLabel];
        }
        return [].concat((0, _toConsumableArray2.default)(list), [SPLIT, keyedLabel]);
      }, []);
    };
    return rawValues.map(function (valueCells) {
      var _valueOptions;
      var valueOptions = (0, _treeUtil.toPathOptions)(valueCells, options, fieldNames);
      var label = mergedDisplayRender(valueOptions.map(function (_ref) {
        var _option$fieldNames$la;
        var option = _ref.option,
          value = _ref.value;
        return (_option$fieldNames$la = option === null || option === void 0 ? void 0 : option[fieldNames.label]) !== null && _option$fieldNames$la !== void 0 ? _option$fieldNames$la : value;
      }), valueOptions.map(function (_ref2) {
        var option = _ref2.option;
        return option;
      }));
      var value = (0, _commonUtil.toPathKey)(valueCells);
      return {
        label: label,
        value: value,
        key: value,
        valueCells: valueCells,
        disabled: (_valueOptions = valueOptions[valueOptions.length - 1]) === null || _valueOptions === void 0 || (_valueOptions = _valueOptions.option) === null || _valueOptions === void 0 ? void 0 : _valueOptions.disabled
      };
    });
  }, [rawValues, options, fieldNames, displayRender, multiple]);
};