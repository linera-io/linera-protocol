"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = TimePanel;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _dateUtil = require("../../utils/dateUtil");
var _context = require("../context");
var _PanelHeader = _interopRequireDefault(require("../PanelHeader"));
var _TimePanelBody = _interopRequireDefault(require("./TimePanelBody"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function TimePanel(props) {
  var prefixCls = props.prefixCls,
    value = props.value,
    locale = props.locale,
    generateConfig = props.generateConfig,
    showTime = props.showTime;
  var _ref = showTime || {},
    format = _ref.format;
  var panelPrefixCls = "".concat(prefixCls, "-time-panel");

  // ========================== Base ==========================
  var _useInfo = (0, _context.useInfo)(props, 'time'),
    _useInfo2 = (0, _slicedToArray2.default)(_useInfo, 1),
    info = _useInfo2[0];

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(_context.PanelContext.Provider, {
    value: info
  }, /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(panelPrefixCls)
  }, /*#__PURE__*/React.createElement(_PanelHeader.default, null, value ? (0, _dateUtil.formatValue)(value, {
    locale: locale,
    format: format,
    generateConfig: generateConfig
  }) : "\xA0"), /*#__PURE__*/React.createElement(_TimePanelBody.default, showTime)));
}