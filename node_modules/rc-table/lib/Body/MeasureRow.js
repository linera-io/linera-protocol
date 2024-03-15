"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = MeasureRow;
var React = _interopRequireWildcard(require("react"));
var _rcResizeObserver = _interopRequireDefault(require("rc-resize-observer"));
var _MeasureCell = _interopRequireDefault(require("./MeasureCell"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function MeasureRow(_ref) {
  var prefixCls = _ref.prefixCls,
    columnsKey = _ref.columnsKey,
    onColumnResize = _ref.onColumnResize;
  return /*#__PURE__*/React.createElement("tr", {
    "aria-hidden": "true",
    className: "".concat(prefixCls, "-measure-row"),
    style: {
      height: 0,
      fontSize: 0
    }
  }, /*#__PURE__*/React.createElement(_rcResizeObserver.default.Collection, {
    onBatchResize: function onBatchResize(infoList) {
      infoList.forEach(function (_ref2) {
        var columnKey = _ref2.data,
          size = _ref2.size;
        onColumnResize(columnKey, size.offsetWidth);
      });
    }
  }, columnsKey.map(function (columnKey) {
    return /*#__PURE__*/React.createElement(_MeasureCell.default, {
      key: columnKey,
      columnKey: columnKey,
      onColumnResize: onColumnResize
    });
  })));
}