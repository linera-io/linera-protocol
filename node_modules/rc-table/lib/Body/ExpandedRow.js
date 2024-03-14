"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _context = require("@rc-component/context");
var React = _interopRequireWildcard(require("react"));
var _Cell = _interopRequireDefault(require("../Cell"));
var _TableContext = _interopRequireDefault(require("../context/TableContext"));
var _useRenderTimes = _interopRequireDefault(require("../hooks/useRenderTimes"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function ExpandedRow(props) {
  if (process.env.NODE_ENV !== 'production') {
    (0, _useRenderTimes.default)(props);
  }
  var prefixCls = props.prefixCls,
    children = props.children,
    Component = props.component,
    cellComponent = props.cellComponent,
    className = props.className,
    expanded = props.expanded,
    colSpan = props.colSpan,
    isEmpty = props.isEmpty;
  var _useContext = (0, _context.useContext)(_TableContext.default, ['scrollbarSize', 'fixHeader', 'fixColumn', 'componentWidth', 'horizonScroll']),
    scrollbarSize = _useContext.scrollbarSize,
    fixHeader = _useContext.fixHeader,
    fixColumn = _useContext.fixColumn,
    componentWidth = _useContext.componentWidth,
    horizonScroll = _useContext.horizonScroll;

  // Cache render node
  var contentNode = children;
  if (isEmpty ? horizonScroll && componentWidth : fixColumn) {
    contentNode = /*#__PURE__*/React.createElement("div", {
      style: {
        width: componentWidth - (fixHeader ? scrollbarSize : 0),
        position: 'sticky',
        left: 0,
        overflow: 'hidden'
      },
      className: "".concat(prefixCls, "-expanded-row-fixed")
    }, contentNode);
  }
  return /*#__PURE__*/React.createElement(Component, {
    className: className,
    style: {
      display: expanded ? null : 'none'
    }
  }, /*#__PURE__*/React.createElement(_Cell.default, {
    component: cellComponent,
    prefixCls: prefixCls,
    colSpan: colSpan
  }, contentNode));
}
var _default = exports.default = ExpandedRow;