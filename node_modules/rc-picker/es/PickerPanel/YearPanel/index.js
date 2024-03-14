import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { formatValue, isInRange, isSameYear } from "../../utils/dateUtil";
import { PanelContext, useInfo } from "../context";
import PanelBody from "../PanelBody";
import PanelHeader from "../PanelHeader";
export default function YearPanel(props) {
  var prefixCls = props.prefixCls,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    disabledDate = props.disabledDate,
    onPickerValueChange = props.onPickerValueChange,
    onModeChange = props.onModeChange;
  var panelPrefixCls = "".concat(prefixCls, "-year-panel");

  // ========================== Base ==========================
  var _useInfo = useInfo(props, 'year'),
    _useInfo2 = _slicedToArray(_useInfo, 1),
    info = _useInfo2[0];
  var getStartYear = function getStartYear(date) {
    var startYear = Math.floor(generateConfig.getYear(pickerValue) / 10) * 10;
    return generateConfig.setYear(date, startYear);
  };
  var getEndYear = function getEndYear(date) {
    var startYear = getStartYear(date);
    return generateConfig.addYear(startYear, 9);
  };
  var startYearDate = getStartYear(pickerValue);
  var endYearDate = getEndYear(pickerValue);
  var baseDate = generateConfig.addYear(startYearDate, -1);

  // ========================= Cells ==========================
  var getCellDate = function getCellDate(date, offset) {
    return generateConfig.addYear(date, offset);
  };
  var getCellText = function getCellText(date) {
    return formatValue(date, {
      locale: locale,
      format: locale.cellYearFormat,
      generateConfig: generateConfig
    });
  };
  var getCellClassName = function getCellClassName(date) {
    return _defineProperty({}, "".concat(prefixCls, "-cell-in-view"), isSameYear(generateConfig, date, startYearDate) || isSameYear(generateConfig, date, endYearDate) || isInRange(generateConfig, startYearDate, endYearDate, date));
  };

  // ======================== Disabled ========================
  var mergedDisabledDate = disabledDate ? function (currentDate, disabledInfo) {
    // Start
    var startMonth = generateConfig.setMonth(currentDate, 0);
    var startDate = generateConfig.setDate(startMonth, 1);

    // End
    var endMonth = generateConfig.setMonth(currentDate, generateConfig.getMonth(currentDate) + 1);
    var enDate = generateConfig.addDate(endMonth, -1);
    return disabledDate(startDate, disabledInfo) && disabledDate(enDate, disabledInfo);
  } : null;

  // ========================= Header =========================
  var yearNode = /*#__PURE__*/React.createElement("button", {
    type: "button",
    key: "year",
    onClick: function onClick() {
      onModeChange('decade');
    },
    tabIndex: -1,
    className: "".concat(prefixCls, "-decade-btn")
  }, formatValue(startYearDate, {
    locale: locale,
    format: locale.yearFormat,
    generateConfig: generateConfig
  }), "-", formatValue(endYearDate, {
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
      return generateConfig.addYear(pickerValue, distance * 10);
    },
    onChange: onPickerValueChange
    // Limitation
    ,
    getStart: getStartYear,
    getEnd: getEndYear
  }, yearNode), /*#__PURE__*/React.createElement(PanelBody, _extends({}, props, {
    disabledDate: mergedDisabledDate,
    titleFormat: locale.fieldYearFormat,
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