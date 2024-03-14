import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { formatValue, isInRange, isSameDecade } from "../../utils/dateUtil";
import { PanelContext, useInfo } from "../context";
import PanelBody from "../PanelBody";
import PanelHeader from "../PanelHeader";
export default function DecadePanel(props) {
  var prefixCls = props.prefixCls,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    disabledDate = props.disabledDate,
    onPickerValueChange = props.onPickerValueChange;
  var panelPrefixCls = "".concat(prefixCls, "-decade-panel");

  // ========================== Base ==========================
  var _useInfo = useInfo(props, 'decade'),
    _useInfo2 = _slicedToArray(_useInfo, 1),
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
    var startYearStr = formatValue(date, {
      locale: locale,
      format: cellYearFormat,
      generateConfig: generateConfig
    });
    var endYearStr = formatValue(generateConfig.addYear(date, 9), {
      locale: locale,
      format: cellYearFormat,
      generateConfig: generateConfig
    });
    return "".concat(startYearStr, "-").concat(endYearStr);
  };
  var getCellClassName = function getCellClassName(date) {
    return _defineProperty({}, "".concat(prefixCls, "-cell-in-view"), isSameDecade(generateConfig, date, startYearDate) || isSameDecade(generateConfig, date, endYearDate) || isInRange(generateConfig, startYearDate, endYearDate, date));
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
  var yearNode = "".concat(formatValue(startYearDate, {
    locale: locale,
    format: locale.yearFormat,
    generateConfig: generateConfig
  }), "-").concat(formatValue(endYearDate, {
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
      return generateConfig.addYear(pickerValue, distance * 100);
    },
    onChange: onPickerValueChange
    // Limitation
    ,
    getStart: getStartYear,
    getEnd: getEndYear
  }, yearNode), /*#__PURE__*/React.createElement(PanelBody, _extends({}, props, {
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