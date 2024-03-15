import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import * as React from 'react';
import { formatValue, isInRange, isSame } from "../utils/dateUtil";
import { PickerHackContext, usePanelContext } from "./context";
export default function PanelBody(props) {
  var rowNum = props.rowNum,
    colNum = props.colNum,
    baseDate = props.baseDate,
    getCellDate = props.getCellDate,
    prefixColumn = props.prefixColumn,
    rowClassName = props.rowClassName,
    titleFormat = props.titleFormat,
    getCellText = props.getCellText,
    getCellClassName = props.getCellClassName,
    headerCells = props.headerCells,
    _props$cellSelection = props.cellSelection,
    cellSelection = _props$cellSelection === void 0 ? true : _props$cellSelection,
    disabledDate = props.disabledDate;
  var _usePanelContext = usePanelContext(),
    prefixCls = _usePanelContext.prefixCls,
    type = _usePanelContext.panelType,
    now = _usePanelContext.now,
    contextDisabledDate = _usePanelContext.disabledDate,
    cellRender = _usePanelContext.cellRender,
    onHover = _usePanelContext.onHover,
    hoverValue = _usePanelContext.hoverValue,
    hoverRangeValue = _usePanelContext.hoverRangeValue,
    generateConfig = _usePanelContext.generateConfig,
    values = _usePanelContext.values,
    locale = _usePanelContext.locale,
    onSelect = _usePanelContext.onSelect;
  var mergedDisabledDate = disabledDate || contextDisabledDate;
  var cellPrefixCls = "".concat(prefixCls, "-cell");

  // ============================= Context ==============================
  var _React$useContext = React.useContext(PickerHackContext),
    onCellDblClick = _React$useContext.onCellDblClick;

  // ============================== Value ===============================
  var matchValues = function matchValues(date) {
    return values.some(function (singleValue) {
      return singleValue && isSame(generateConfig, locale, date, singleValue, type);
    });
  };

  // =============================== Body ===============================
  var rows = [];
  for (var row = 0; row < rowNum; row += 1) {
    var rowNode = [];
    var rowStartDate = void 0;
    var _loop = function _loop() {
      var offset = row * colNum + col;
      var currentDate = getCellDate(baseDate, offset);
      var disabled = mergedDisabledDate === null || mergedDisabledDate === void 0 ? void 0 : mergedDisabledDate(currentDate, {
        type: type
      });

      // Row Start Cell
      if (col === 0) {
        rowStartDate = currentDate;
        if (prefixColumn) {
          rowNode.push(prefixColumn(rowStartDate));
        }
      }

      // Range
      var inRange = false;
      var rangeStart = false;
      var rangeEnd = false;
      if (cellSelection && hoverRangeValue) {
        var _hoverRangeValue = _slicedToArray(hoverRangeValue, 2),
          hoverStart = _hoverRangeValue[0],
          hoverEnd = _hoverRangeValue[1];
        inRange = isInRange(generateConfig, hoverStart, hoverEnd, currentDate);
        rangeStart = isSame(generateConfig, locale, currentDate, hoverStart, type);
        rangeEnd = isSame(generateConfig, locale, currentDate, hoverEnd, type);
      }

      // Title
      var title = titleFormat ? formatValue(currentDate, {
        locale: locale,
        format: titleFormat,
        generateConfig: generateConfig
      }) : undefined;

      // Render
      var inner = /*#__PURE__*/React.createElement("div", {
        className: "".concat(cellPrefixCls, "-inner")
      }, getCellText(currentDate));
      rowNode.push( /*#__PURE__*/React.createElement("td", {
        key: col,
        title: title,
        className: classNames(cellPrefixCls, _objectSpread(_defineProperty(_defineProperty(_defineProperty(_defineProperty(_defineProperty(_defineProperty({}, "".concat(cellPrefixCls, "-disabled"), disabled), "".concat(cellPrefixCls, "-hover"), (hoverValue || []).some(function (date) {
          return isSame(generateConfig, locale, currentDate, date, type);
        })), "".concat(cellPrefixCls, "-in-range"), inRange && !rangeStart && !rangeEnd), "".concat(cellPrefixCls, "-range-start"), rangeStart), "".concat(cellPrefixCls, "-range-end"), rangeEnd), "".concat(prefixCls, "-cell-selected"), !hoverRangeValue &&
        // WeekPicker use row instead
        type !== 'week' && matchValues(currentDate)), getCellClassName(currentDate))),
        onClick: function onClick() {
          if (!disabled) {
            onSelect(currentDate);
          }
        },
        onDoubleClick: function onDoubleClick() {
          if (!disabled && onCellDblClick) {
            onCellDblClick();
          }
        },
        onMouseEnter: function onMouseEnter() {
          if (!disabled) {
            onHover === null || onHover === void 0 || onHover(currentDate);
          }
        },
        onMouseLeave: function onMouseLeave() {
          if (!disabled) {
            onHover === null || onHover === void 0 || onHover(null);
          }
        }
      }, cellRender ? cellRender(currentDate, {
        prefixCls: prefixCls,
        originNode: inner,
        today: now,
        type: type,
        locale: locale
      }) : inner));
    };
    for (var col = 0; col < colNum; col += 1) {
      _loop();
    }
    rows.push( /*#__PURE__*/React.createElement("tr", {
      key: row,
      className: rowClassName === null || rowClassName === void 0 ? void 0 : rowClassName(rowStartDate)
    }, rowNode));
  }

  // ============================== Render ==============================
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-body")
  }, /*#__PURE__*/React.createElement("table", {
    className: "".concat(prefixCls, "-content")
  }, headerCells && /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, headerCells)), /*#__PURE__*/React.createElement("tbody", null, rows)));
}