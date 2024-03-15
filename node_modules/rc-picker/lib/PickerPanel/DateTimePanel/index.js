"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = DateTimePanel;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _useTimeInfo3 = _interopRequireDefault(require("../../hooks/useTimeInfo"));
var _dateUtil = require("../../utils/dateUtil");
var _DatePanel = _interopRequireDefault(require("../DatePanel"));
var _TimePanel = _interopRequireDefault(require("../TimePanel"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function DateTimePanel(props) {
  var prefixCls = props.prefixCls,
    generateConfig = props.generateConfig,
    showTime = props.showTime,
    onSelect = props.onSelect,
    value = props.value,
    pickerValue = props.pickerValue,
    onHover = props.onHover;
  var panelPrefixCls = "".concat(prefixCls, "-datetime-panel");

  // =============================== Time ===============================
  var _useTimeInfo = (0, _useTimeInfo3.default)(generateConfig, showTime),
    _useTimeInfo2 = (0, _slicedToArray2.default)(_useTimeInfo, 1),
    getValidTime = _useTimeInfo2[0];

  // Merge the time info from `value` or `pickerValue`
  var mergeTime = function mergeTime(date) {
    if (value) {
      return (0, _dateUtil.fillTime)(generateConfig, date, value);
    }
    return (0, _dateUtil.fillTime)(generateConfig, date, pickerValue);
  };

  // ============================== Hover ===============================
  var onDateHover = function onDateHover(date) {
    onHover(date ? mergeTime(date) : date);
  };

  // ============================== Select ==============================
  var onDateSelect = function onDateSelect(date) {
    // Merge with current time
    var cloneDate = mergeTime(date);
    onSelect(getValidTime(cloneDate, cloneDate));
  };

  // ============================== Render ==============================
  return /*#__PURE__*/React.createElement("div", {
    className: panelPrefixCls
  }, /*#__PURE__*/React.createElement(_DatePanel.default, (0, _extends2.default)({}, props, {
    onSelect: onDateSelect,
    onHover: onDateHover
  })), /*#__PURE__*/React.createElement(_TimePanel.default, props));
}