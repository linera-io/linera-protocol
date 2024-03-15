"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread3 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcResizeObserver = _interopRequireDefault(require("rc-resize-observer"));
var _rcUtil = require("rc-util");
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../context"));
var _useInputProps3 = _interopRequireDefault(require("./hooks/useInputProps"));
var _useRootProps = _interopRequireDefault(require("./hooks/useRootProps"));
var _Icon = _interopRequireWildcard(require("./Icon"));
var _Input = _interopRequireDefault(require("./Input"));
var _excluded = ["id", "clearIcon", "suffixIcon", "separator", "activeIndex", "activeHelp", "allHelp", "focused", "onFocus", "onBlur", "onKeyDown", "locale", "generateConfig", "placeholder", "className", "style", "onClick", "onClear", "value", "onChange", "onSubmit", "onInputChange", "format", "maskFormat", "preserveInvalidOnBlur", "onInvalid", "disabled", "invalid", "inputReadOnly", "direction", "onOpenChange", "onActiveOffset", "onMouseDown", "required", "aria-required", "autoFocus"],
  _excluded2 = ["index"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function RangeSelector(props, ref) {
  var id = props.id,
    clearIcon = props.clearIcon,
    suffixIcon = props.suffixIcon,
    _props$separator = props.separator,
    separator = _props$separator === void 0 ? '~' : _props$separator,
    activeIndex = props.activeIndex,
    activeHelp = props.activeHelp,
    allHelp = props.allHelp,
    focused = props.focused,
    onFocus = props.onFocus,
    onBlur = props.onBlur,
    onKeyDown = props.onKeyDown,
    locale = props.locale,
    generateConfig = props.generateConfig,
    placeholder = props.placeholder,
    className = props.className,
    style = props.style,
    onClick = props.onClick,
    onClear = props.onClear,
    value = props.value,
    onChange = props.onChange,
    onSubmit = props.onSubmit,
    onInputChange = props.onInputChange,
    format = props.format,
    maskFormat = props.maskFormat,
    preserveInvalidOnBlur = props.preserveInvalidOnBlur,
    onInvalid = props.onInvalid,
    disabled = props.disabled,
    invalid = props.invalid,
    inputReadOnly = props.inputReadOnly,
    direction = props.direction,
    onOpenChange = props.onOpenChange,
    onActiveOffset = props.onActiveOffset,
    _onMouseDown = props.onMouseDown,
    required = props.required,
    ariaRequired = props['aria-required'],
    autoFocus = props.autoFocus,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var rtl = direction === 'rtl';

  // ======================== Prefix ========================
  var _React$useContext = React.useContext(_context.default),
    prefixCls = _React$useContext.prefixCls;

  // ========================== Id ==========================
  var ids = React.useMemo(function () {
    if (typeof id === 'string') {
      return [id];
    }
    var mergedId = id || {};
    return [mergedId.start, mergedId.end];
  }, [id]);

  // ========================= Refs =========================
  var rootRef = React.useRef();
  var inputStartRef = React.useRef();
  var inputEndRef = React.useRef();
  var getInput = function getInput(index) {
    var _index;
    return (_index = [inputStartRef, inputEndRef][index]) === null || _index === void 0 ? void 0 : _index.current;
  };
  React.useImperativeHandle(ref, function () {
    return {
      nativeElement: rootRef.current,
      focus: function focus(options) {
        if ((0, _typeof2.default)(options) === 'object') {
          var _getInput;
          var _ref = options || {},
            _ref$index = _ref.index,
            _index2 = _ref$index === void 0 ? 0 : _ref$index,
            rest = (0, _objectWithoutProperties2.default)(_ref, _excluded2);
          (_getInput = getInput(_index2)) === null || _getInput === void 0 || _getInput.focus(rest);
        } else {
          var _getInput2;
          (_getInput2 = getInput(options !== null && options !== void 0 ? options : 0)) === null || _getInput2 === void 0 || _getInput2.focus();
        }
      },
      blur: function blur() {
        var _getInput3, _getInput4;
        (_getInput3 = getInput(0)) === null || _getInput3 === void 0 || _getInput3.blur();
        (_getInput4 = getInput(1)) === null || _getInput4 === void 0 || _getInput4.blur();
      }
    };
  });

  // ======================== Props =========================
  var rootProps = (0, _useRootProps.default)(restProps);

  // ===================== Placeholder ======================
  var mergedPlaceholder = React.useMemo(function () {
    return Array.isArray(placeholder) ? placeholder : [placeholder, placeholder];
  }, [placeholder]);

  // ======================== Inputs ========================
  var _useInputProps = (0, _useInputProps3.default)((0, _objectSpread3.default)((0, _objectSpread3.default)({}, props), {}, {
      id: ids,
      placeholder: mergedPlaceholder
    })),
    _useInputProps2 = (0, _slicedToArray2.default)(_useInputProps, 1),
    getInputProps = _useInputProps2[0];

  // ====================== ActiveBar =======================
  var offsetUnit = rtl ? 'right' : 'left';
  var _React$useState = React.useState((0, _defineProperty2.default)({
      position: 'absolute',
      width: 0
    }, offsetUnit, 0)),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    activeBarStyle = _React$useState2[0],
    setActiveBarStyle = _React$useState2[1];
  var syncActiveOffset = (0, _rcUtil.useEvent)(function () {
    var input = getInput(activeIndex);
    if (input) {
      var _input$nativeElement = input.nativeElement,
        offsetWidth = _input$nativeElement.offsetWidth,
        offsetLeft = _input$nativeElement.offsetLeft,
        offsetParent = _input$nativeElement.offsetParent;
      var _offset = offsetLeft;
      if (rtl) {
        var parentElement = offsetParent;
        var parentStyle = getComputedStyle(parentElement);
        _offset = parentElement.offsetWidth - parseFloat(parentStyle.borderRightWidth) - parseFloat(parentStyle.borderLeftWidth) - offsetLeft - offsetWidth;
      }
      setActiveBarStyle(function (ori) {
        return (0, _objectSpread3.default)((0, _objectSpread3.default)({}, ori), {}, (0, _defineProperty2.default)({
          width: offsetWidth
        }, offsetUnit, _offset));
      });
      onActiveOffset(activeIndex === 0 ? 0 : _offset);
    }
  });
  React.useEffect(function () {
    syncActiveOffset();
  }, [activeIndex]);

  // ======================== Clear =========================
  var showClear = clearIcon && (value[0] && !disabled[0] || value[1] && !disabled[1]);

  // ======================= Disabled =======================
  var startAutoFocus = autoFocus && !disabled[0];
  var endAutoFocus = autoFocus && !startAutoFocus && !disabled[1];

  // ======================== Render ========================
  return /*#__PURE__*/React.createElement(_rcResizeObserver.default, {
    onResize: syncActiveOffset
  }, /*#__PURE__*/React.createElement("div", (0, _extends2.default)({}, rootProps, {
    className: (0, _classnames.default)(prefixCls, "".concat(prefixCls, "-range"), (0, _defineProperty2.default)((0, _defineProperty2.default)((0, _defineProperty2.default)((0, _defineProperty2.default)({}, "".concat(prefixCls, "-focused"), focused), "".concat(prefixCls, "-disabled"), disabled.every(function (i) {
      return i;
    })), "".concat(prefixCls, "-invalid"), invalid.some(function (i) {
      return i;
    })), "".concat(prefixCls, "-rtl"), rtl), className),
    style: style,
    ref: rootRef,
    onClick: onClick
    // Not lose current input focus
    ,
    onMouseDown: function onMouseDown(e) {
      var target = e.target;
      if (target !== inputStartRef.current.inputElement && target !== inputEndRef.current.inputElement) {
        e.preventDefault();
      }
      _onMouseDown === null || _onMouseDown === void 0 || _onMouseDown(e);
    }
  }), /*#__PURE__*/React.createElement(_Input.default, (0, _extends2.default)({
    ref: inputStartRef
  }, getInputProps(0), {
    autoFocus: startAutoFocus,
    "date-range": "start"
  })), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-range-separator")
  }, separator), /*#__PURE__*/React.createElement(_Input.default, (0, _extends2.default)({
    ref: inputEndRef
  }, getInputProps(1), {
    autoFocus: endAutoFocus,
    "date-range": "end"
  })), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-active-bar"),
    style: activeBarStyle
  }), /*#__PURE__*/React.createElement(_Icon.default, {
    type: "suffix",
    icon: suffixIcon
  }), showClear && /*#__PURE__*/React.createElement(_Icon.ClearIcon, {
    icon: clearIcon,
    onClear: onClear
  })));
}
var RefRangeSelector = /*#__PURE__*/React.forwardRef(RangeSelector);
if (process.env.NODE_ENV !== 'production') {
  RefRangeSelector.displayName = 'RangeSelector';
}
var _default = exports.default = RefRangeSelector;