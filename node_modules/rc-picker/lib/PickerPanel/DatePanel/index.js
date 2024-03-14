"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = DatePanel;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _dateUtil = require("../../utils/dateUtil");
var _context = require("../context");
var _PanelBody = _interopRequireDefault(require("../PanelBody"));
var _PanelHeader = _interopRequireDefault(require("../PanelHeader"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function DatePanel(props) {
  var prefixCls = props.prefixCls,
    _props$panelName = props.panelName,
    panelName = _props$panelName === void 0 ? 'date' : _props$panelName,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    onPickerValueChange = props.onPickerValueChange,
    onModeChange = props.onModeChange,
    _props$mode = props.mode,
    mode = _props$mode === void 0 ? 'date' : _props$mode,
    disabledDate = props.disabledDate,
    onSelect = props.onSelect,
    onHover = props.onHover,
    showWeek = props.showWeek;
  var panelPrefixCls = "".concat(prefixCls, "-").concat(panelName, "-panel");
  var cellPrefixCls = "".concat(prefixCls, "-cell");
  var isWeek = mode === 'week';

  // ========================== Base ==========================
  var _useInfo = (0, _context.useInfo)(props, mode),
    _useInfo2 = (0, _slicedToArray2.default)(_useInfo, 2),
    info = _useInfo2[0],
    now = _useInfo2[1];
  var weekFirstDay = generateConfig.locale.getWeekFirstDay(locale.locale);
  var monthStartDate = generateConfig.setDate(pickerValue, 1);
  var baseDate = (0, _dateUtil.getWeekStartDate)(locale.locale, generateConfig, monthStartDate);
  var month = generateConfig.getMonth(pickerValue);

  // =========================== PrefixColumn ===========================
  var prefixColumn = isWeek || showWeek ? function (date) {
    // >>> Additional check for disabled
    var disabled = disabledDate === null || disabledDate === void 0 ? void 0 : disabledDate(date, {
      type: 'week'
    });
    return /*#__PURE__*/React.createElement("td", {
      key: "week",
      className: (0, _classnames.default)(cellPrefixCls, "".concat(cellPrefixCls, "-week"), (0, _defineProperty2.default)({}, "".concat(cellPrefixCls, "-disabled"), disabled))
      // Operation: Same as code in PanelBody
      ,
      onClick: function onClick() {
        if (!disabled) {
          onSelect(date);
        }
      },
      onMouseEnter: function onMouseEnter() {
        if (!disabled) {
          onHover === null || onHover === void 0 || onHover(date);
        }
      },
      onMouseLeave: function onMouseLeave() {
        if (!disabled) {
          onHover === null || onHover === void 0 || onHover(null);
        }
      }
    }, /*#__PURE__*/React.createElement("div", {
      className: "".concat(cellPrefixCls, "-inner")
    }, generateConfig.locale.getWeek(locale.locale, date)));
  } : null;

  // ========================= Cells ==========================
  // >>> Header Cells
  var headerCells = [];
  var weekDaysLocale = locale.shortWeekDays || (generateConfig.locale.getShortWeekDays ? generateConfig.locale.getShortWeekDays(locale.locale) : []);
  if (prefixColumn) {
    headerCells.push( /*#__PURE__*/React.createElement("th", {
      key: "empty",
      "aria-label": "empty cell"
    }));
  }
  for (var i = 0; i < _dateUtil.WEEK_DAY_COUNT; i += 1) {
    headerCells.push( /*#__PURE__*/React.createElement("th", {
      key: i
    }, weekDaysLocale[(i + weekFirstDay) % _dateUtil.WEEK_DAY_COUNT]));
  }

  // >>> Body Cells
  var getCellDate = function getCellDate(date, offset) {
    return generateConfig.addDate(date, offset);
  };
  var getCellText = function getCellText(date) {
    return (0, _dateUtil.formatValue)(date, {
      locale: locale,
      format: locale.cellDateFormat,
      generateConfig: generateConfig
    });
  };
  var getCellClassName = function getCellClassName(date) {
    var classObj = (0, _defineProperty2.default)((0, _defineProperty2.default)({}, "".concat(prefixCls, "-cell-in-view"), (0, _dateUtil.isSameMonth)(generateConfig, date, pickerValue)), "".concat(prefixCls, "-cell-today"), (0, _dateUtil.isSameDate)(generateConfig, date, now));
    return classObj;
  };

  // ========================= Header =========================
  var monthsLocale = locale.shortMonths || (generateConfig.locale.getShortMonths ? generateConfig.locale.getShortMonths(locale.locale) : []);
  var yearNode = /*#__PURE__*/React.createElement("button", {
    type: "button",
    key: "year",
    onClick: function onClick() {
      onModeChange('year', pickerValue);
    },
    tabIndex: -1,
    className: "".concat(prefixCls, "-year-btn")
  }, (0, _dateUtil.formatValue)(pickerValue, {
    locale: locale,
    format: locale.yearFormat,
    generateConfig: generateConfig
  }));
  var monthNode = /*#__PURE__*/React.createElement("button", {
    type: "button",
    key: "month",
    onClick: function onClick() {
      onModeChange('month', pickerValue);
    },
    tabIndex: -1,
    className: "".concat(prefixCls, "-month-btn")
  }, locale.monthFormat ? (0, _dateUtil.formatValue)(pickerValue, {
    locale: locale,
    format: locale.monthFormat,
    generateConfig: generateConfig
  }) : monthsLocale[month]);
  var monthYearNodes = locale.monthBeforeYear ? [monthNode, yearNode] : [yearNode, monthNode];

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(_context.PanelContext.Provider, {
    value: info
  }, /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(panelPrefixCls, showWeek && "".concat(panelPrefixCls, "-show-week"))
  }, /*#__PURE__*/React.createElement(_PanelHeader.default, {
    offset: function offset(distance) {
      return generateConfig.addMonth(pickerValue, distance);
    },
    superOffset: function superOffset(distance) {
      return generateConfig.addYear(pickerValue, distance);
    },
    onChange: onPickerValueChange
    // Limitation
    ,
    getStart: function getStart(date) {
      return generateConfig.setDate(date, 1);
    },
    getEnd: function getEnd(date) {
      var clone = generateConfig.setDate(date, 1);
      clone = generateConfig.addMonth(clone, 1);
      return generateConfig.addDate(clone, -1);
    }
  }, monthYearNodes), /*#__PURE__*/React.createElement(_PanelBody.default, (0, _extends2.default)({
    titleFormat: locale.fieldDateFormat
  }, props, {
    colNum: _dateUtil.WEEK_DAY_COUNT,
    rowNum: 6,
    baseDate: baseDate
    // Header
    ,
    headerCells: headerCells
    // Body
    ,
    getCellDate: getCellDate,
    getCellText: getCellText,
    getCellClassName: getCellClassName,
    prefixColumn: prefixColumn,
    cellSelection: !isWeek
  }))));
}