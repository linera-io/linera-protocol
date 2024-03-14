"use strict";

var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = PresetPanel;
var React = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function executeValue(value) {
  return typeof value === 'function' ? value() : value;
}
function PresetPanel(props) {
  var prefixCls = props.prefixCls,
    presets = props.presets,
    _onClick = props.onClick,
    onHover = props.onHover;
  if (!presets.length) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-presets")
  }, /*#__PURE__*/React.createElement("ul", null, presets.map(function (_ref, index) {
    var label = _ref.label,
      value = _ref.value;
    return /*#__PURE__*/React.createElement("li", {
      key: index,
      onClick: function onClick() {
        _onClick(executeValue(value));
      },
      onMouseEnter: function onMouseEnter() {
        onHover(executeValue(value));
      },
      onMouseLeave: function onMouseLeave() {
        onHover(null);
      }
    }, label);
  })));
}