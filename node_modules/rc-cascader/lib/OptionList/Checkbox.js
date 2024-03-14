"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Checkbox;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _context = _interopRequireDefault(require("../context"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Checkbox(_ref) {
  var _classNames;
  var prefixCls = _ref.prefixCls,
    checked = _ref.checked,
    halfChecked = _ref.halfChecked,
    disabled = _ref.disabled,
    onClick = _ref.onClick,
    disableCheckbox = _ref.disableCheckbox;
  var _React$useContext = React.useContext(_context.default),
    checkable = _React$useContext.checkable;
  var customCheckbox = typeof checkable !== 'boolean' ? checkable : null;
  return /*#__PURE__*/React.createElement("span", {
    className: (0, _classnames.default)("".concat(prefixCls), (_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-checked"), checked), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-indeterminate"), !checked && halfChecked), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-disabled"), disabled || disableCheckbox), _classNames)),
    onClick: onClick
  }, customCheckbox);
}