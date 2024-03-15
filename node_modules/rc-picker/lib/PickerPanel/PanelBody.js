"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = PanelBody;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread3 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _dateUtil = require("../utils/dateUtil");
var _context = require("./context");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function PanelBody(props) {
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
  var _usePanelContext = (0, _context.usePanelContext)(),
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
  var _React$useContext = React.useContext(_context.PickerHackContext),
    onCellDblClick = _React$useContext.onCellDblClick;

  // ============================== Value ===============================
  var matchValues = function matchValues(date) {
    return values.some(function (singleValue) {
      return singleValue && (0, _dateUtil.isSame)(generateConfig, locale, date, singleValue, type);
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
        var _hoverRangeValue = (0, _slicedToArray2.default)(hoverRangeValue, 2),
          hoverStart = _hoverRangeValue[0],
          hoverEnd = _hoverRangeValue[1];
        inRange = (0, _dateUtil.isInRange)(generateConfig, hoverStart, hoverEnd, currentDate);
        rangeStart = (0, _dateUtil.isSame)(generateConfig, locale, currentDate, hoverStart, type);
        rangeEnd = (0, _dateUtil.isSame)(generateConfig, locale, currentDate, hoverEnd, type);
      }

      // Title
      var title = titleFormat ? (0, _dateUtil.formatValue)(currentDate, {
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
        className: (0, _classnames.default)(cellPrefixCls, (0, _objectSpread3.default)((0, _defineProperty2.default)((0, _defineProperty2.default)((0, _defineProperty2.default)((0, _defineProperty2.default)((0, _defineProperty2.default)((0, _defineProperty2.default)({}, "".concat(cellPrefixCls, "-disabled"), disabled), "".concat(cellPrefixCls, "-hover"), (hoverValue || []).some(function (date) {
          return (0, _dateUtil.isSame)(generateConfig, locale, currentDate, date, type);
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