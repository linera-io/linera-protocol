import _extends from "@babel/runtime/helpers/esm/extends";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import useTimeInfo from "../../hooks/useTimeInfo";
import { fillTime } from "../../utils/dateUtil";
import DatePanel from "../DatePanel";
import TimePanel from "../TimePanel";
export default function DateTimePanel(props) {
  var prefixCls = props.prefixCls,
    generateConfig = props.generateConfig,
    showTime = props.showTime,
    onSelect = props.onSelect,
    value = props.value,
    pickerValue = props.pickerValue,
    onHover = props.onHover;
  var panelPrefixCls = "".concat(prefixCls, "-datetime-panel");

  // =============================== Time ===============================
  var _useTimeInfo = useTimeInfo(generateConfig, showTime),
    _useTimeInfo2 = _slicedToArray(_useTimeInfo, 1),
    getValidTime = _useTimeInfo2[0];

  // Merge the time info from `value` or `pickerValue`
  var mergeTime = function mergeTime(date) {
    if (value) {
      return fillTime(generateConfig, date, value);
    }
    return fillTime(generateConfig, date, pickerValue);
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
  }, /*#__PURE__*/React.createElement(DatePanel, _extends({}, props, {
    onSelect: onDateSelect,
    onHover: onDateHover
  })), /*#__PURE__*/React.createElement(TimePanel, props));
}