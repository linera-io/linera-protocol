"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ClearIcon = ClearIcon;
exports.default = Icon;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../context"));
var _excluded = ["icon", "type"],
  _excluded2 = ["onClear"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Icon(props) {
  var icon = props.icon,
    type = props.type,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var _React$useContext = React.useContext(_context.default),
    prefixCls = _React$useContext.prefixCls;
  return icon ? /*#__PURE__*/React.createElement("span", (0, _extends2.default)({
    className: "".concat(prefixCls, "-").concat(type)
  }, restProps), icon) : null;
}
function ClearIcon(_ref) {
  var onClear = _ref.onClear,
    restProps = (0, _objectWithoutProperties2.default)(_ref, _excluded2);
  return /*#__PURE__*/React.createElement(Icon, (0, _extends2.default)({}, restProps, {
    type: "clear",
    role: "button",
    onMouseDown: function onMouseDown(e) {
      e.preventDefault();
    },
    onClick: function onClick(e) {
      e.stopPropagation();
      onClear();
    }
  }));
}