import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import * as React from 'react';
import { formatValue, getWeekStartDate, isSameDate, isSameMonth, WEEK_DAY_COUNT } from "../../utils/dateUtil";
import { PanelContext, useInfo } from "../context";
import PanelBody from "../PanelBody";
import PanelHeader from "../PanelHeader";
export default function DatePanel(props) {
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
  var _useInfo = useInfo(props, mode),
    _useInfo2 = _slicedToArray(_useInfo, 2),
    info = _useInfo2[0],
    now = _useInfo2[1];
  var weekFirstDay = generateConfig.locale.getWeekFirstDay(locale.locale);
  var monthStartDate = generateConfig.setDate(pickerValue, 1);
  var baseDate = getWeekStartDate(locale.locale, generateConfig, monthStartDate);
  var month = generateConfig.getMonth(pickerValue);

  // =========================== PrefixColumn ===========================
  var prefixColumn = isWeek || showWeek ? function (date) {
    // >>> Additional check for disabled
    var disabled = disabledDate === null || disabledDate === void 0 ? void 0 : disabledDate(date, {
      type: 'week'
    });
    return /*#__PURE__*/React.createElement("td", {
      key: "week",
      className: classNames(cellPrefixCls, "".concat(cellPrefixCls, "-week"), _defineProperty({}, "".concat(cellPrefixCls, "-disabled"), disabled))
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
  for (var i = 0; i < WEEK_DAY_COUNT; i += 1) {
    headerCells.push( /*#__PURE__*/React.createElement("th", {
      key: i
    }, weekDaysLocale[(i + weekFirstDay) % WEEK_DAY_COUNT]));
  }

  // >>> Body Cells
  var getCellDate = function getCellDate(date, offset) {
    return generateConfig.addDate(date, offset);
  };
  var getCellText = function getCellText(date) {
    return formatValue(date, {
      locale: locale,
      format: locale.cellDateFormat,
      generateConfig: generateConfig
    });
  };
  var getCellClassName = function getCellClassName(date) {
    var classObj = _defineProperty(_defineProperty({}, "".concat(prefixCls, "-cell-in-view"), isSameMonth(generateConfig, date, pickerValue)), "".concat(prefixCls, "-cell-today"), isSameDate(generateConfig, date, now));
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
  }, formatValue(pickerValue, {
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
  }, locale.monthFormat ? formatValue(pickerValue, {
    locale: locale,
    format: locale.monthFormat,
    generateConfig: generateConfig
  }) : monthsLocale[month]);
  var monthYearNodes = locale.monthBeforeYear ? [monthNode, yearNode] : [yearNode, monthNode];

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(PanelContext.Provider, {
    value: info
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames(panelPrefixCls, showWeek && "".concat(panelPrefixCls, "-show-week"))
  }, /*#__PURE__*/React.createElement(PanelHeader, {
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
  }, monthYearNodes), /*#__PURE__*/React.createElement(PanelBody, _extends({
    titleFormat: locale.fieldDateFormat
  }, props, {
    colNum: WEEK_DAY_COUNT,
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