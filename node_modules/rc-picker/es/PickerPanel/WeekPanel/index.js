import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import * as React from 'react';
import { isInRange, isSameWeek } from "../../utils/dateUtil";
import DatePanel from "../DatePanel";
export default function WeekPanel(props) {
  var prefixCls = props.prefixCls,
    generateConfig = props.generateConfig,
    locale = props.locale,
    value = props.value,
    hoverValue = props.hoverValue,
    hoverRangeValue = props.hoverRangeValue;

  // =============================== Row ================================
  var localeName = locale.locale;
  var rowPrefixCls = "".concat(prefixCls, "-week-panel-row");
  var rowClassName = function rowClassName(currentDate) {
    var rangeCls = {};
    if (hoverRangeValue) {
      var _hoverRangeValue = _slicedToArray(hoverRangeValue, 2),
        rangeStart = _hoverRangeValue[0],
        rangeEnd = _hoverRangeValue[1];
      var isRangeStart = isSameWeek(generateConfig, localeName, rangeStart, currentDate);
      var isRangeEnd = isSameWeek(generateConfig, localeName, rangeEnd, currentDate);
      rangeCls["".concat(rowPrefixCls, "-range-start")] = isRangeStart;
      rangeCls["".concat(rowPrefixCls, "-range-end")] = isRangeEnd;
      rangeCls["".concat(rowPrefixCls, "-range-hover")] = !isRangeStart && !isRangeEnd && isInRange(generateConfig, rangeStart, rangeEnd, currentDate);
    }
    if (hoverValue) {
      rangeCls["".concat(rowPrefixCls, "-hover")] = hoverValue.some(function (date) {
        return isSameWeek(generateConfig, localeName, currentDate, date);
      });
    }
    return classNames(rowPrefixCls, _defineProperty({}, "".concat(rowPrefixCls, "-selected"), !hoverRangeValue && isSameWeek(generateConfig, localeName, value, currentDate)),
    // Patch for hover range
    rangeCls);
  };

  // ============================== Render ==============================
  return /*#__PURE__*/React.createElement(DatePanel, _extends({}, props, {
    mode: "week",
    panelName: "week",
    rowClassName: rowClassName
  }));
}