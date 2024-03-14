"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useFieldsInvalidate;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _miscUtil = require("../../utils/miscUtil");
var React = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/**
 * Used to control each fields invalidate status
 */
function useFieldsInvalidate(calendarValue, isInvalidateDate) {
  var allowEmpty = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var _React$useState = React.useState([false, false]),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    fieldsInvalidates = _React$useState2[0],
    setFieldsInvalidates = _React$useState2[1];
  var onSelectorInvalid = function onSelectorInvalid(invalid, index) {
    setFieldsInvalidates(function (ori) {
      return (0, _miscUtil.fillIndex)(ori, index, invalid);
    });
  };

  /**
   * For the Selector Input to mark as `aria-disabled`
   */
  var submitInvalidates = React.useMemo(function () {
    return fieldsInvalidates.map(function (invalid, index) {
      // If typing invalidate
      if (invalid) {
        return true;
      }
      var current = calendarValue[index];

      // Not check if all empty
      if (!current) {
        return false;
      }

      // Not allow empty
      if (!allowEmpty[index] && !current) {
        return true;
      }

      // Invalidate
      if (current && isInvalidateDate(current, {
        activeIndex: index
      })) {
        return true;
      }
      return false;
    });
  }, [calendarValue, fieldsInvalidates, isInvalidateDate, allowEmpty]);
  return [submitInvalidates, onSelectorInvalid];
}