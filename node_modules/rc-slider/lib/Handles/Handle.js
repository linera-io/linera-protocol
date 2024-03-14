"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../context"));
var _util = require("../util");
var _excluded = ["prefixCls", "value", "valueIndex", "onStartMove", "style", "render", "dragging", "onOffsetChange", "onChangeComplete"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var Handle = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _cls, _getIndex;
  var prefixCls = props.prefixCls,
    value = props.value,
    valueIndex = props.valueIndex,
    onStartMove = props.onStartMove,
    style = props.style,
    render = props.render,
    dragging = props.dragging,
    onOffsetChange = props.onOffsetChange,
    onChangeComplete = props.onChangeComplete,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var _React$useContext = React.useContext(_context.default),
    min = _React$useContext.min,
    max = _React$useContext.max,
    direction = _React$useContext.direction,
    disabled = _React$useContext.disabled,
    keyboard = _React$useContext.keyboard,
    range = _React$useContext.range,
    tabIndex = _React$useContext.tabIndex,
    ariaLabelForHandle = _React$useContext.ariaLabelForHandle,
    ariaLabelledByForHandle = _React$useContext.ariaLabelledByForHandle,
    ariaValueTextFormatterForHandle = _React$useContext.ariaValueTextFormatterForHandle,
    styles = _React$useContext.styles,
    classNames = _React$useContext.classNames;
  var handlePrefixCls = "".concat(prefixCls, "-handle");

  // ============================ Events ============================
  var onInternalStartMove = function onInternalStartMove(e) {
    if (!disabled) {
      onStartMove(e, valueIndex);
    }
  };

  // =========================== Keyboard ===========================
  var onKeyDown = function onKeyDown(e) {
    if (!disabled && keyboard) {
      var offset = null;

      // Change the value
      switch (e.which || e.keyCode) {
        case _KeyCode.default.LEFT:
          offset = direction === 'ltr' || direction === 'btt' ? -1 : 1;
          break;
        case _KeyCode.default.RIGHT:
          offset = direction === 'ltr' || direction === 'btt' ? 1 : -1;
          break;

        // Up is plus
        case _KeyCode.default.UP:
          offset = direction !== 'ttb' ? 1 : -1;
          break;

        // Down is minus
        case _KeyCode.default.DOWN:
          offset = direction !== 'ttb' ? -1 : 1;
          break;
        case _KeyCode.default.HOME:
          offset = 'min';
          break;
        case _KeyCode.default.END:
          offset = 'max';
          break;
        case _KeyCode.default.PAGE_UP:
          offset = 2;
          break;
        case _KeyCode.default.PAGE_DOWN:
          offset = -2;
          break;
      }
      if (offset !== null) {
        e.preventDefault();
        onOffsetChange(offset, valueIndex);
      }
    }
  };
  var handleKeyUp = function handleKeyUp(e) {
    switch (e.which || e.keyCode) {
      case _KeyCode.default.LEFT:
      case _KeyCode.default.RIGHT:
      case _KeyCode.default.UP:
      case _KeyCode.default.DOWN:
      case _KeyCode.default.HOME:
      case _KeyCode.default.END:
      case _KeyCode.default.PAGE_UP:
      case _KeyCode.default.PAGE_DOWN:
        onChangeComplete === null || onChangeComplete === void 0 || onChangeComplete();
        break;
    }
  };

  // ============================ Offset ============================
  var positionStyle = (0, _util.getDirectionStyle)(direction, value, min, max);

  // ============================ Render ============================
  var handleNode = /*#__PURE__*/React.createElement("div", (0, _extends2.default)({
    ref: ref,
    className: (0, _classnames.default)(handlePrefixCls, (_cls = {}, (0, _defineProperty2.default)(_cls, "".concat(handlePrefixCls, "-").concat(valueIndex + 1), range), (0, _defineProperty2.default)(_cls, "".concat(handlePrefixCls, "-dragging"), dragging), _cls), classNames.handle),
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)((0, _objectSpread2.default)({}, positionStyle), style), styles.handle),
    onMouseDown: onInternalStartMove,
    onTouchStart: onInternalStartMove,
    onKeyDown: onKeyDown,
    onKeyUp: handleKeyUp,
    tabIndex: disabled ? null : (0, _util.getIndex)(tabIndex, valueIndex),
    role: "slider",
    "aria-valuemin": min,
    "aria-valuemax": max,
    "aria-valuenow": value,
    "aria-disabled": disabled,
    "aria-label": (0, _util.getIndex)(ariaLabelForHandle, valueIndex),
    "aria-labelledby": (0, _util.getIndex)(ariaLabelledByForHandle, valueIndex),
    "aria-valuetext": (_getIndex = (0, _util.getIndex)(ariaValueTextFormatterForHandle, valueIndex)) === null || _getIndex === void 0 ? void 0 : _getIndex(value),
    "aria-orientation": direction === 'ltr' || direction === 'rtl' ? 'horizontal' : 'vertical'
  }, restProps));

  // Customize
  if (render) {
    handleNode = render(handleNode, {
      index: valueIndex,
      prefixCls: prefixCls,
      value: value,
      dragging: dragging
    });
  }
  return handleNode;
});
if (process.env.NODE_ENV !== 'production') {
  Handle.displayName = 'Handle';
}
var _default = exports.default = Handle;