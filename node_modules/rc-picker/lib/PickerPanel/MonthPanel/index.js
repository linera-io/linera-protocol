"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = MonthPanel;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _dateUtil = require("../../utils/dateUtil");
var _context = require("../context");
var _PanelBody = _interopRequireDefault(require("../PanelBody"));
var _PanelHeader = _interopRequireDefault(require("../PanelHeader"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function MonthPanel(props) {
  var prefixCls = props.prefixCls,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    disabledDate = props.disabledDate,
    onPickerValueChange = props.onPickerValueChange,
    onModeChange = props.onModeChange;
  var panelPrefixCls = "".concat(prefixCls, "-month-panel");

  // ========================== Base ==========================
  var _useInfo = (0, _context.useInfo)(props, 'month'),
    _useInfo2 = (0, _slicedToArray2.default)(_useInfo, 1),
    info = _useInfo2[0];
  var baseDate = generateConfig.setMonth(pickerValue, 0);

  // ========================= Month ==========================
  var monthsLocale = locale.shortMonths || (generateConfig.locale.getShortMonths ? generateConfig.locale.getShortMonths(locale.locale) : []);

  // ========================= Cells ==========================
  var getCellDate = function getCellDate(date, offset) {
    return generateConfig.addMonth(date, offset);
  };
  var getCellText = function getCellText(date) {
    var month = generateConfig.getMonth(date);
    return locale.monthFormat ? (0, _dateUtil.formatValue)(date, {
      locale: locale,
      format: locale.monthFormat,
      generateConfig: generateConfig
    }) : monthsLocale[month];
  };
  var getCellClassName = function getCellClassName() {
    return (0, _defineProperty2.default)({}, "".concat(prefixCls, "-cell-in-view"), true);
  };

  // ======================== Disabled ========================
  var mergedDisabledDate = disabledDate ? function (currentDate, disabledInfo) {
    var startDate = generateConfig.setDate(currentDate, 1);
    var nextMonthStartDate = generateConfig.setMonth(startDate, generateConfig.getMonth(startDate) + 1);
    var endDate = generateConfig.addDate(nextMonthStartDate, -1);
    return disabledDate(startDate, disabledInfo) && disabledDate(endDate, disabledInfo);
  } : null;

  // ========================= Header =========================
  var yearNode = /*#__PURE__*/React.createElement("button", {
    type: "button",
    key: "year",
    onClick: function onClick() {
      onModeChange('year');
    },
    tabIndex: -1,
    className: "".concat(prefixCls, "-year-btn")
  }, (0, _dateUtil.formatValue)(pickerValue, {
    locale: locale,
    format: locale.yearFormat,
    generateConfig: generateConfig
  }));

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(_context.PanelContext.Provider, {
    value: info
  }, /*#__PURE__*/React.createElement("div", {
    className: panelPrefixCls
  }, /*#__PURE__*/React.createElement(_PanelHeader.default, {
    superOffset: function superOffset(distance) {
      return generateConfig.addYear(pickerValue, distance);
    },
    onChange: onPickerValueChange
    // Limitation
    ,
    getStart: function getStart(date) {
      return generateConfig.setMonth(date, 0);
    },
    getEnd: function getEnd(date) {
      return generateConfig.setMonth(date, 11);
    }
  }, yearNode), /*#__PURE__*/React.createElement(_PanelBody.default, (0, _extends2.default)({}, props, {
    disabledDate: mergedDisabledDate,
    titleFormat: locale.fieldMonthFormat,
    colNum: 3,
    rowNum: 4,
    baseDate: baseDate
    // Body
    ,
    getCellDate: getCellDate,
    getCellText: getCellText,
    getCellClassName: getCellClassName
  }))));
}