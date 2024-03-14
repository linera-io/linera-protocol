"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Divider;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _MenuContext = require("./context/MenuContext");
var _PathContext = require("./context/PathContext");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Divider(_ref) {
  var className = _ref.className,
    style = _ref.style;
  var _React$useContext = React.useContext(_MenuContext.MenuContext),
    prefixCls = _React$useContext.prefixCls;
  var measure = (0, _PathContext.useMeasure)();
  if (measure) {
    return null;
  }
  return /*#__PURE__*/React.createElement("li", {
    role: "separator",
    className: (0, _classnames.default)("".concat(prefixCls, "-item-divider"), className),
    style: style
  });
}