import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { formatValue } from "../../utils/dateUtil";
import { PanelContext, useInfo } from "../context";
import PanelBody from "../PanelBody";
import PanelHeader from "../PanelHeader";
export default function QuarterPanel(props) {
  var prefixCls = props.prefixCls,
    locale = props.locale,
    generateConfig = props.generateConfig,
    pickerValue = props.pickerValue,
    onPickerValueChange = props.onPickerValueChange,
    onModeChange = props.onModeChange;
  var panelPrefixCls = "".concat(prefixCls, "-quarter-panel");

  // ========================== Base ==========================
  var _useInfo = useInfo(props, 'quarter'),
    _useInfo2 = _slicedToArray(_useInfo, 1),
    info = _useInfo2[0];
  var baseDate = generateConfig.setMonth(pickerValue, 0);

  // ========================= Cells ==========================
  var getCellDate = function getCellDate(date, offset) {
    return generateConfig.addMonth(date, offset * 3);
  };
  var getCellText = function getCellText(date) {
    return formatValue(date, {
      locale: locale,
      format: locale.cellQuarterFormat,
      generateConfig: generateConfig
    });
  };
  var getCellClassName = function getCellClassName() {
    return _defineProperty({}, "".concat(prefixCls, "-cell-in-view"), true);
  };

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
    titleFormat: locale.fieldQuarterFormat,
    colNum: 4,
    rowNum: 1,
    baseDate: baseDate
    // Body
    ,
    getCellDate: getCellDate,
    getCellText: getCellText,
    getCellClassName: getCellClassName
  }))));
}