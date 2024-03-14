"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = DecadePanel;
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
function DecadePanel(props) {
  var prefixCls = props.prefixCls,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    disabledDate = props.disabledDate,
    onPickerValueChange = props.onPickerValueChange;
  var panelPrefixCls = "".concat(prefixCls, "-decade-panel");

  // ========================== Base ==========================
  var _useInfo = (0, _context.useInfo)(props, 'decade'),
    _useInfo2 = (0, _slicedToArray2.default)(_useInfo, 1),
    info = _useInfo2[0];
  var getStartYear = function getStartYear(date) {
    var startYear = Math.floor(generateConfig.getYear(pickerValue) / 100) * 100;
    return generateConfig.setYear(date, startYear);
  };
  var getEndYear = function getEndYear(date) {
    var startYear = getStartYear(date);
    return generateConfig.addYear(startYear, 99);
  };
  var startYearDate = getStartYear(pickerValue);
  var endYearDate = getEndYear(pickerValue);
  var baseDate = generateConfig.addYear(startYearDate, -10);

  // ========================= Cells ==========================
  var getCellDate = function getCellDate(date, offset) {
    return generateConfig.addYear(date, offset * 10);
  };
  var getCellText = function getCellText(date) {
    var cellYearFormat = locale.cellYearFormat;
    var startYearStr = (0, _dateUtil.formatValue)(date, {
      locale: locale,
      format: cellYearFormat,
      generateConfig: generateConfig
    });
    var endYearStr = (0, _dateUtil.formatValue)(generateConfig.addYear(date, 9), {
      locale: locale,
      format: cellYearFormat,
      generateConfig: generateConfig
    });
    return "".concat(startYearStr, "-").concat(endYearStr);
  };
  var getCellClassName = function getCellClassName(date) {
    return (0, _defineProperty2.default)({}, "".concat(prefixCls, "-cell-in-view"), (0, _dateUtil.isSameDecade)(generateConfig, date, startYearDate) || (0, _dateUtil.isSameDecade)(generateConfig, date, endYearDate) || (0, _dateUtil.isInRange)(generateConfig, startYearDate, endYearDate, date));
  };

  // ======================== Disabled ========================
  var mergedDisabledDate = disabledDate ? function (currentDate, disabledInfo) {
    // Start
    var baseStartDate = generateConfig.setDate(currentDate, 1);
    var baseStartMonth = generateConfig.setMonth(baseStartDate, 0);
    var baseStartYear = generateConfig.setYear(baseStartMonth, Math.floor(generateConfig.getYear(baseStartMonth) / 10) * 10);

    // End
    var baseEndYear = generateConfig.addYear(baseStartYear, 10);
    var baseEndDate = generateConfig.addDate(baseEndYear, -1);
    return disabledDate(baseStartYear, disabledInfo) && disabledDate(baseEndDate, disabledInfo);
  } : null;

  // ========================= Header =========================
  var yearNode = "".concat((0, _dateUtil.formatValue)(startYearDate, {
    locale: locale,
    format: locale.yearFormat,
    generateConfig: generateConfig
  }), "-").concat((0, _dateUtil.formatValue)(endYearDate, {
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
      return generateConfig.addYear(pickerValue, distance * 100);
    },
    onChange: onPickerValueChange
    // Limitation
    ,
    getStart: getStartYear,
    getEnd: getEndYear
  }, yearNode), /*#__PURE__*/React.createElement(_PanelBody.default, (0, _extends2.default)({}, props, {
    disabledDate: mergedDisabledDate,
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