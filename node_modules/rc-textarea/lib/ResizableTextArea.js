"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcResizeObserver = _interopRequireDefault(require("rc-resize-observer"));
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _useMergedState3 = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var React = _interopRequireWildcard(require("react"));
var _calculateNodeHeight = _interopRequireDefault(require("./calculateNodeHeight"));
var _excluded = ["prefixCls", "onPressEnter", "defaultValue", "value", "autoSize", "onResize", "className", "style", "disabled", "onChange", "onInternalAutoSize"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var RESIZE_START = 0;
var RESIZE_MEASURING = 1;
var RESIZE_STABLE = 2;
var ResizableTextArea = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _ref = props,
    prefixCls = _ref.prefixCls,
    onPressEnter = _ref.onPressEnter,
    defaultValue = _ref.defaultValue,
    value = _ref.value,
    autoSize = _ref.autoSize,
    onResize = _ref.onResize,
    className = _ref.className,
    style = _ref.style,
    disabled = _ref.disabled,
    onChange = _ref.onChange,
    onInternalAutoSize = _ref.onInternalAutoSize,
    restProps = (0, _objectWithoutProperties2.default)(_ref, _excluded);

  // =============================== Value ================================
  var _useMergedState = (0, _useMergedState3.default)(defaultValue, {
      value: value,
      postState: function postState(val) {
        return val !== null && val !== void 0 ? val : '';
      }
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    mergedValue = _useMergedState2[0],
    setMergedValue = _useMergedState2[1];
  var onInternalChange = function onInternalChange(event) {
    setMergedValue(event.target.value);
    onChange === null || onChange === void 0 || onChange(event);
  };

  // ================================ Ref =================================
  var textareaRef = React.useRef();
  React.useImperativeHandle(ref, function () {
    return {
      textArea: textareaRef.current
    };
  });

  // ============================== AutoSize ==============================
  var _React$useMemo = React.useMemo(function () {
      if (autoSize && (0, _typeof2.default)(autoSize) === 'object') {
        return [autoSize.minRows, autoSize.maxRows];
      }
      return [];
    }, [autoSize]),
    _React$useMemo2 = (0, _slicedToArray2.default)(_React$useMemo, 2),
    minRows = _React$useMemo2[0],
    maxRows = _React$useMemo2[1];
  var needAutoSize = !!autoSize;

  // =============================== Scroll ===============================
  // https://github.com/ant-design/ant-design/issues/21870
  var fixFirefoxAutoScroll = function fixFirefoxAutoScroll() {
    try {
      // FF has bug with jump of scroll to top. We force back here.
      if (document.activeElement === textareaRef.current) {
        var _textareaRef$current = textareaRef.current,
          selectionStart = _textareaRef$current.selectionStart,
          selectionEnd = _textareaRef$current.selectionEnd,
          scrollTop = _textareaRef$current.scrollTop;

        // Fix Safari bug which not rollback when break line
        // This makes Chinese IME can't input. Do not fix this
        // const { value: tmpValue } = textareaRef.current;
        // textareaRef.current.value = '';
        // textareaRef.current.value = tmpValue;

        textareaRef.current.setSelectionRange(selectionStart, selectionEnd);
        textareaRef.current.scrollTop = scrollTop;
      }
    } catch (e) {
      // Fix error in Chrome:
      // Failed to read the 'selectionStart' property from 'HTMLInputElement'
      // http://stackoverflow.com/q/21177489/3040605
    }
  };

  // =============================== Resize ===============================
  var _React$useState = React.useState(RESIZE_STABLE),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    resizeState = _React$useState2[0],
    setResizeState = _React$useState2[1];
  var _React$useState3 = React.useState(),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    autoSizeStyle = _React$useState4[0],
    setAutoSizeStyle = _React$useState4[1];
  var startResize = function startResize() {
    setResizeState(RESIZE_START);
    if (process.env.NODE_ENV === 'test') {
      onInternalAutoSize === null || onInternalAutoSize === void 0 || onInternalAutoSize();
    }
  };

  // Change to trigger resize measure
  (0, _useLayoutEffect.default)(function () {
    if (needAutoSize) {
      startResize();
    }
  }, [value, minRows, maxRows, needAutoSize]);
  (0, _useLayoutEffect.default)(function () {
    if (resizeState === RESIZE_START) {
      setResizeState(RESIZE_MEASURING);
    } else if (resizeState === RESIZE_MEASURING) {
      var textareaStyles = (0, _calculateNodeHeight.default)(textareaRef.current, false, minRows, maxRows);

      // Safari has bug that text will keep break line on text cut when it's prev is break line.
      // ZombieJ: This not often happen. So we just skip it.
      // const { selectionStart, selectionEnd, scrollTop } = textareaRef.current;
      // const { value: tmpValue } = textareaRef.current;
      // textareaRef.current.value = '';
      // textareaRef.current.value = tmpValue;

      // if (document.activeElement === textareaRef.current) {
      //   textareaRef.current.scrollTop = scrollTop;
      //   textareaRef.current.setSelectionRange(selectionStart, selectionEnd);
      // }

      setResizeState(RESIZE_STABLE);
      setAutoSizeStyle(textareaStyles);
    } else {
      fixFirefoxAutoScroll();
    }
  }, [resizeState]);

  // We lock resize trigger by raf to avoid Safari warning
  var resizeRafRef = React.useRef();
  var cleanRaf = function cleanRaf() {
    _raf.default.cancel(resizeRafRef.current);
  };
  var onInternalResize = function onInternalResize(size) {
    if (resizeState === RESIZE_STABLE) {
      onResize === null || onResize === void 0 || onResize(size);
      if (autoSize) {
        cleanRaf();
        resizeRafRef.current = (0, _raf.default)(function () {
          startResize();
        });
      }
    }
  };
  React.useEffect(function () {
    return cleanRaf;
  }, []);

  // =============================== Render ===============================
  var mergedAutoSizeStyle = needAutoSize ? autoSizeStyle : null;
  var mergedStyle = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, style), mergedAutoSizeStyle);
  if (resizeState === RESIZE_START || resizeState === RESIZE_MEASURING) {
    mergedStyle.overflowY = 'hidden';
    mergedStyle.overflowX = 'hidden';
  }
  return /*#__PURE__*/React.createElement(_rcResizeObserver.default, {
    onResize: onInternalResize,
    disabled: !(autoSize || onResize)
  }, /*#__PURE__*/React.createElement("textarea", (0, _extends2.default)({}, restProps, {
    ref: textareaRef,
    style: mergedStyle,
    className: (0, _classnames.default)(prefixCls, className, (0, _defineProperty2.default)({}, "".concat(prefixCls, "-disabled"), disabled)),
    disabled: disabled,
    value: mergedValue,
    onChange: onInternalChange
  })));
});
var _default = exports.default = ResizableTextArea;