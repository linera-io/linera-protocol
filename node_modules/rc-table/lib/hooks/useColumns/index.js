"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.convertChildrenToColumns = convertChildrenToColumns;
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var React = _interopRequireWildcard(require("react"));
var _constant = require("../../constant");
var _legacyUtil = require("../../utils/legacyUtil");
var _useWidthColumns3 = _interopRequireDefault(require("./useWidthColumns"));
var _excluded = ["children"],
  _excluded2 = ["fixed"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function convertChildrenToColumns(children) {
  return (0, _toArray.default)(children).filter(function (node) {
    return /*#__PURE__*/React.isValidElement(node);
  }).map(function (_ref) {
    var key = _ref.key,
      props = _ref.props;
    var nodeChildren = props.children,
      restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
    var column = (0, _objectSpread2.default)({
      key: key
    }, restProps);
    if (nodeChildren) {
      column.children = convertChildrenToColumns(nodeChildren);
    }
    return column;
  });
}
function filterHiddenColumns(columns) {
  return columns.filter(function (column) {
    return column && (0, _typeof2.default)(column) === 'object' && !column.hidden;
  }).map(function (column) {
    var subColumns = column.children;
    if (subColumns && subColumns.length > 0) {
      return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, column), {}, {
        children: filterHiddenColumns(subColumns)
      });
    }
    return column;
  });
}
function flatColumns(columns) {
  var parentKey = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 'key';
  return columns.filter(function (column) {
    return column && (0, _typeof2.default)(column) === 'object';
  }).reduce(function (list, column, index) {
    var fixed = column.fixed;
    // Convert `fixed='true'` to `fixed='left'` instead
    var parsedFixed = fixed === true ? 'left' : fixed;
    var mergedKey = "".concat(parentKey, "-").concat(index);
    var subColumns = column.children;
    if (subColumns && subColumns.length > 0) {
      return [].concat((0, _toConsumableArray2.default)(list), (0, _toConsumableArray2.default)(flatColumns(subColumns, mergedKey).map(function (subColum) {
        return (0, _objectSpread2.default)({
          fixed: parsedFixed
        }, subColum);
      })));
    }
    return [].concat((0, _toConsumableArray2.default)(list), [(0, _objectSpread2.default)((0, _objectSpread2.default)({
      key: mergedKey
    }, column), {}, {
      fixed: parsedFixed
    })]);
  }, []);
}
function revertForRtl(columns) {
  return columns.map(function (column) {
    var fixed = column.fixed,
      restProps = (0, _objectWithoutProperties2.default)(column, _excluded2);

    // Convert `fixed='left'` to `fixed='right'` instead
    var parsedFixed = fixed;
    if (fixed === 'left') {
      parsedFixed = 'right';
    } else if (fixed === 'right') {
      parsedFixed = 'left';
    }
    return (0, _objectSpread2.default)({
      fixed: parsedFixed
    }, restProps);
  });
}

/**
 * Parse `columns` & `children` into `columns`.
 */
function useColumns(_ref2, transformColumns) {
  var prefixCls = _ref2.prefixCls,
    columns = _ref2.columns,
    children = _ref2.children,
    expandable = _ref2.expandable,
    expandedKeys = _ref2.expandedKeys,
    columnTitle = _ref2.columnTitle,
    getRowKey = _ref2.getRowKey,
    onTriggerExpand = _ref2.onTriggerExpand,
    expandIcon = _ref2.expandIcon,
    rowExpandable = _ref2.rowExpandable,
    expandIconColumnIndex = _ref2.expandIconColumnIndex,
    direction = _ref2.direction,
    expandRowByClick = _ref2.expandRowByClick,
    columnWidth = _ref2.columnWidth,
    fixed = _ref2.fixed,
    scrollWidth = _ref2.scrollWidth,
    clientWidth = _ref2.clientWidth;
  var baseColumns = React.useMemo(function () {
    var newColumns = columns || convertChildrenToColumns(children) || [];
    return filterHiddenColumns(newColumns.slice());
  }, [columns, children]);

  // ========================== Expand ==========================
  var withExpandColumns = React.useMemo(function () {
    if (expandable) {
      var _expandColumn;
      var cloneColumns = baseColumns.slice();

      // >>> Warning if use `expandIconColumnIndex`
      if (process.env.NODE_ENV !== 'production' && expandIconColumnIndex >= 0) {
        (0, _warning.default)(false, '`expandIconColumnIndex` is deprecated. Please use `Table.EXPAND_COLUMN` in `columns` instead.');
      }

      // >>> Insert expand column if not exist
      if (!cloneColumns.includes(_constant.EXPAND_COLUMN)) {
        var expandColIndex = expandIconColumnIndex || 0;
        if (expandColIndex >= 0) {
          cloneColumns.splice(expandColIndex, 0, _constant.EXPAND_COLUMN);
        }
      }

      // >>> Deduplicate additional expand column
      if (process.env.NODE_ENV !== 'production' && cloneColumns.filter(function (c) {
        return c === _constant.EXPAND_COLUMN;
      }).length > 1) {
        (0, _warning.default)(false, 'There exist more than one `EXPAND_COLUMN` in `columns`.');
      }
      var expandColumnIndex = cloneColumns.indexOf(_constant.EXPAND_COLUMN);
      cloneColumns = cloneColumns.filter(function (column, index) {
        return column !== _constant.EXPAND_COLUMN || index === expandColumnIndex;
      });

      // >>> Check if expand column need to fixed
      var prevColumn = baseColumns[expandColumnIndex];
      var fixedColumn;
      if ((fixed === 'left' || fixed) && !expandIconColumnIndex) {
        fixedColumn = 'left';
      } else if ((fixed === 'right' || fixed) && expandIconColumnIndex === baseColumns.length) {
        fixedColumn = 'right';
      } else {
        fixedColumn = prevColumn ? prevColumn.fixed : null;
      }

      // >>> Create expandable column
      var expandColumn = (_expandColumn = {}, (0, _defineProperty2.default)(_expandColumn, _legacyUtil.INTERNAL_COL_DEFINE, {
        className: "".concat(prefixCls, "-expand-icon-col"),
        columnType: 'EXPAND_COLUMN'
      }), (0, _defineProperty2.default)(_expandColumn, "title", columnTitle), (0, _defineProperty2.default)(_expandColumn, "fixed", fixedColumn), (0, _defineProperty2.default)(_expandColumn, "className", "".concat(prefixCls, "-row-expand-icon-cell")), (0, _defineProperty2.default)(_expandColumn, "width", columnWidth), (0, _defineProperty2.default)(_expandColumn, "render", function render(_, record, index) {
        var rowKey = getRowKey(record, index);
        var expanded = expandedKeys.has(rowKey);
        var recordExpandable = rowExpandable ? rowExpandable(record) : true;
        var icon = expandIcon({
          prefixCls: prefixCls,
          expanded: expanded,
          expandable: recordExpandable,
          record: record,
          onExpand: onTriggerExpand
        });
        if (expandRowByClick) {
          return /*#__PURE__*/React.createElement("span", {
            onClick: function onClick(e) {
              return e.stopPropagation();
            }
          }, icon);
        }
        return icon;
      }), _expandColumn);
      return cloneColumns.map(function (col) {
        return col === _constant.EXPAND_COLUMN ? expandColumn : col;
      });
    }
    if (process.env.NODE_ENV !== 'production' && baseColumns.includes(_constant.EXPAND_COLUMN)) {
      (0, _warning.default)(false, '`expandable` is not config but there exist `EXPAND_COLUMN` in `columns`.');
    }
    return baseColumns.filter(function (col) {
      return col !== _constant.EXPAND_COLUMN;
    });
  }, [expandable, baseColumns, getRowKey, expandedKeys, expandIcon, direction]);

  // ========================= Transform ========================
  var mergedColumns = React.useMemo(function () {
    var finalColumns = withExpandColumns;
    if (transformColumns) {
      finalColumns = transformColumns(finalColumns);
    }

    // Always provides at least one column for table display
    if (!finalColumns.length) {
      finalColumns = [{
        render: function render() {
          return null;
        }
      }];
    }
    return finalColumns;
  }, [transformColumns, withExpandColumns, direction]);

  // ========================== Flatten =========================
  var flattenColumns = React.useMemo(function () {
    if (direction === 'rtl') {
      return revertForRtl(flatColumns(mergedColumns));
    }
    return flatColumns(mergedColumns);
  }, [mergedColumns, direction, scrollWidth]);

  // ========================= Gap Fixed ========================
  var hasGapFixed = React.useMemo(function () {
    // Fixed: left, since old browser not support `findLastIndex`, we should use reverse loop
    var lastLeftIndex = -1;
    for (var i = flattenColumns.length - 1; i >= 0; i -= 1) {
      var colFixed = flattenColumns[i].fixed;
      if (colFixed === 'left' || colFixed === true) {
        lastLeftIndex = i;
        break;
      }
    }
    if (lastLeftIndex >= 0) {
      for (var _i = 0; _i <= lastLeftIndex; _i += 1) {
        var _colFixed = flattenColumns[_i].fixed;
        if (_colFixed !== 'left' && _colFixed !== true) {
          return true;
        }
      }
    }

    // Fixed: right
    var firstRightIndex = flattenColumns.findIndex(function (_ref3) {
      var colFixed = _ref3.fixed;
      return colFixed === 'right';
    });
    if (firstRightIndex >= 0) {
      for (var _i2 = firstRightIndex; _i2 < flattenColumns.length; _i2 += 1) {
        var _colFixed2 = flattenColumns[_i2].fixed;
        if (_colFixed2 !== 'right') {
          return true;
        }
      }
    }
    return false;
  }, [flattenColumns]);

  // ========================= FillWidth ========================
  var _useWidthColumns = (0, _useWidthColumns3.default)(flattenColumns, scrollWidth, clientWidth),
    _useWidthColumns2 = (0, _slicedToArray2.default)(_useWidthColumns, 2),
    filledColumns = _useWidthColumns2[0],
    realScrollWidth = _useWidthColumns2[1];
  return [mergedColumns, filledColumns, realScrollWidth, hasGapFixed];
}
var _default = exports.default = useColumns;