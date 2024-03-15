"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var React = _interopRequireWildcard(require("react"));
var _legacyUtil = require("./utils/legacyUtil");
var _excluded = ["columnType"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function ColGroup(_ref) {
  var colWidths = _ref.colWidths,
    columns = _ref.columns,
    columCount = _ref.columCount;
  var cols = [];
  var len = columCount || columns.length;

  // Only insert col with width & additional props
  // Skip if rest col do not have any useful info
  var mustInsert = false;
  for (var i = len - 1; i >= 0; i -= 1) {
    var width = colWidths[i];
    var column = columns && columns[i];
    var additionalProps = column && column[_legacyUtil.INTERNAL_COL_DEFINE];
    if (width || additionalProps || mustInsert) {
      var _ref2 = additionalProps || {},
        columnType = _ref2.columnType,
        restAdditionalProps = (0, _objectWithoutProperties2.default)(_ref2, _excluded);
      cols.unshift( /*#__PURE__*/React.createElement("col", (0, _extends2.default)({
        key: i,
        style: {
          width: width
        }
      }, restAdditionalProps)));
      mustInsert = true;
    }
  }
  return /*#__PURE__*/React.createElement("colgroup", null, cols);
}
var _default = exports.default = ColGroup;