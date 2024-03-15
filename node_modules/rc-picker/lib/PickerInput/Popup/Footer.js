"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Footer;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _useTimeInfo3 = _interopRequireDefault(require("../../hooks/useTimeInfo"));
var _context = _interopRequireDefault(require("../context"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Footer(props) {
  var mode = props.mode,
    internalMode = props.internalMode,
    renderExtraFooter = props.renderExtraFooter,
    showNow = props.showNow,
    showTime = props.showTime,
    onSubmit = props.onSubmit,
    onNow = props.onNow,
    invalid = props.invalid,
    needConfirm = props.needConfirm,
    generateConfig = props.generateConfig,
    disabledDate = props.disabledDate;
  var _React$useContext = React.useContext(_context.default),
    prefixCls = _React$useContext.prefixCls,
    locale = _React$useContext.locale,
    _React$useContext$but = _React$useContext.button,
    Button = _React$useContext$but === void 0 ? 'button' : _React$useContext$but;

  // >>> Now
  var now = generateConfig.getNow();
  var _useTimeInfo = (0, _useTimeInfo3.default)(generateConfig, showTime, now),
    _useTimeInfo2 = (0, _slicedToArray2.default)(_useTimeInfo, 1),
    getValidTime = _useTimeInfo2[0];

  // ======================== Extra =========================
  var extraNode = renderExtraFooter === null || renderExtraFooter === void 0 ? void 0 : renderExtraFooter(mode);

  // ======================== Ranges ========================
  var nowDisabled = disabledDate(now, {
    type: mode
  });
  var onInternalNow = function onInternalNow() {
    if (!nowDisabled) {
      var validateNow = getValidTime(now);
      onNow(validateNow);
    }
  };
  var nowPrefixCls = "".concat(prefixCls, "-now");
  var nowBtnPrefixCls = "".concat(nowPrefixCls, "-btn");
  var presetNode = showNow && /*#__PURE__*/React.createElement("li", {
    className: nowPrefixCls
  }, /*#__PURE__*/React.createElement("a", {
    className: (0, _classnames.default)(nowBtnPrefixCls, nowDisabled && "".concat(nowBtnPrefixCls, "-disabled")),
    "aria-disabled": nowDisabled,
    onClick: onInternalNow
  }, internalMode === 'date' ? locale.today : locale.now));

  // >>> OK
  var okNode = needConfirm && /*#__PURE__*/React.createElement("li", {
    className: "".concat(prefixCls, "-ok")
  }, /*#__PURE__*/React.createElement(Button, {
    disabled: invalid,
    onClick: onSubmit
  }, locale.ok));
  var rangeNode = (presetNode || okNode) && /*#__PURE__*/React.createElement("ul", {
    className: "".concat(prefixCls, "-ranges")
  }, presetNode, okNode);

  // ======================== Render ========================
  if (!extraNode && !rangeNode) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-footer")
  }, extraNode && /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-footer-extra")
  }, extraNode), rangeNode);
}