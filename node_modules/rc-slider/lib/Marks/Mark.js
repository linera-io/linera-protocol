"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Mark;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _util = require("../util");
var _context = _interopRequireDefault(require("../context"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Mark(props) {
  var prefixCls = props.prefixCls,
    style = props.style,
    children = props.children,
    value = props.value,
    _onClick = props.onClick;
  var _React$useContext = React.useContext(_context.default),
    min = _React$useContext.min,
    max = _React$useContext.max,
    direction = _React$useContext.direction,
    includedStart = _React$useContext.includedStart,
    includedEnd = _React$useContext.includedEnd,
    included = _React$useContext.included;
  var textCls = "".concat(prefixCls, "-text");

  // ============================ Offset ============================
  var positionStyle = (0, _util.getDirectionStyle)(direction, value, min, max);
  return /*#__PURE__*/React.createElement("span", {
    className: (0, _classnames.default)(textCls, (0, _defineProperty2.default)({}, "".concat(textCls, "-active"), included && includedStart <= value && value <= includedEnd)),
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, positionStyle), style),
    onMouseDown: function onMouseDown(e) {
      e.stopPropagation();
    },
    onClick: function onClick() {
      _onClick(value);
    }
  }, children);
}