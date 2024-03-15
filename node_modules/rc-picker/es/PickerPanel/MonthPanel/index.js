import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { formatValue } from "../../utils/dateUtil";
import { PanelContext, useInfo } from "../context";
import PanelBody from "../PanelBody";
import PanelHeader from "../PanelHeader";
export default function MonthPanel(props) {
  var prefixCls = props.prefixCls,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    disabledDate = props.disabledDate,
    onPickerValueChange = props.onPickerValueChange,
    onModeChange = props.onModeChange;
  var panelPrefixCls = "".concat(prefixCls, "-month-panel");

  // ========================== Base ==========================
  var _useInfo = useInfo(props, 'month'),
    _useInfo2 = _slicedToArray(_useInfo, 1),
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
    return locale.monthFormat ? formatValue(date, {
      locale: locale,
      format: locale.monthFormat,
      generateConfig: generateConfig
    }) : monthsLocale[month];
  };
  var getCellClassName = function getCellClassName() {
    return _defineProperty({}, "".concat(prefixCls, "-cell-in-view"), true);
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
  }, formatValue(pickerValue, {
    locale: locale,
    format: locale.yearFormat,
    generateConfig: generateConfig
  }));

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(PanelContext.Provider, {
    value: info
  }, /*#__PURE__*/React.createElement("div", {
    className: panelPrefixCls
  }, /*#__PURE__*/React.createElement(PanelHeader, {
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
  }, yearNode), /*#__PURE__*/React.createElement(PanelBody, _extends({}, props, {
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