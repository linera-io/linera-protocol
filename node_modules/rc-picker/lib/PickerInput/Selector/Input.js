"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcUtil = require("rc-util");
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var React = _interopRequireWildcard(require("react"));
var _miscUtil = require("../../utils/miscUtil");
var _context = _interopRequireDefault(require("../context"));
var _useLockEffect = _interopRequireDefault(require("../hooks/useLockEffect"));
var _Icon = _interopRequireDefault(require("./Icon"));
var _MaskFormat = _interopRequireDefault(require("./MaskFormat"));
var _util = require("./util");
var _excluded = ["active", "showActiveCls", "suffixIcon", "format", "validateFormat", "onChange", "onInput", "helped", "onHelp", "onSubmit", "onKeyDown", "preserveInvalidOnBlur", "invalid", "clearIcon"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
// Format logic
//
// First time on focus:
//  1. check if the text is valid, if not fill with format
//  2. set highlight cell to the first cell
// Cells
//  1. Selection the index cell, set inner `cacheValue` to ''
//  2. Key input filter non-number char, patch after the `cacheValue`
//    1. Replace the `cacheValue` with input align the cell length
//    2. Re-selection the mask cell
//  3. If `cacheValue` match the limit length or cell format (like 1 ~ 12 month), go to next cell

var Input = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var active = props.active,
    _props$showActiveCls = props.showActiveCls,
    showActiveCls = _props$showActiveCls === void 0 ? true : _props$showActiveCls,
    suffixIcon = props.suffixIcon,
    format = props.format,
    validateFormat = props.validateFormat,
    onChange = props.onChange,
    onInput = props.onInput,
    helped = props.helped,
    onHelp = props.onHelp,
    onSubmit = props.onSubmit,
    onKeyDown = props.onKeyDown,
    _props$preserveInvali = props.preserveInvalidOnBlur,
    preserveInvalidOnBlur = _props$preserveInvali === void 0 ? false : _props$preserveInvali,
    invalid = props.invalid,
    clearIcon = props.clearIcon,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var value = props.value,
    onFocus = props.onFocus,
    onBlur = props.onBlur,
    onMouseUp = props.onMouseUp;
  var _React$useContext = React.useContext(_context.default),
    prefixCls = _React$useContext.prefixCls,
    _React$useContext$inp = _React$useContext.input,
    Component = _React$useContext$inp === void 0 ? 'input' : _React$useContext$inp;
  var inputPrefixCls = "".concat(prefixCls, "-input");

  // ======================== Value =========================
  var _React$useState = React.useState(false),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    focused = _React$useState2[0],
    setFocused = _React$useState2[1];
  var _React$useState3 = React.useState(value),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    internalInputValue = _React$useState4[0],
    setInputValue = _React$useState4[1];
  var _React$useState5 = React.useState(''),
    _React$useState6 = (0, _slicedToArray2.default)(_React$useState5, 2),
    focusCellText = _React$useState6[0],
    setFocusCellText = _React$useState6[1];
  var _React$useState7 = React.useState(null),
    _React$useState8 = (0, _slicedToArray2.default)(_React$useState7, 2),
    focusCellIndex = _React$useState8[0],
    setFocusCellIndex = _React$useState8[1];
  var _React$useState9 = React.useState(null),
    _React$useState10 = (0, _slicedToArray2.default)(_React$useState9, 2),
    forceSelectionSyncMark = _React$useState10[0],
    forceSelectionSync = _React$useState10[1];
  var inputValue = internalInputValue || '';

  // Sync value if needed
  React.useEffect(function () {
    setInputValue(value);
  }, [value]);

  // ========================= Refs =========================
  var holderRef = React.useRef();
  var inputRef = React.useRef();
  React.useImperativeHandle(ref, function () {
    return {
      nativeElement: holderRef.current,
      inputElement: inputRef.current,
      focus: function focus(options) {
        inputRef.current.focus(options);
      },
      blur: function blur() {
        inputRef.current.blur();
      }
    };
  });

  // ======================== Format ========================
  var maskFormat = React.useMemo(function () {
    return new _MaskFormat.default(format || '');
  }, [format]);
  var _React$useMemo = React.useMemo(function () {
      if (helped) {
        return [0, 0];
      }
      return maskFormat.getSelection(focusCellIndex);
    }, [maskFormat, focusCellIndex, helped]),
    _React$useMemo2 = (0, _slicedToArray2.default)(_React$useMemo, 2),
    selectionStart = _React$useMemo2[0],
    selectionEnd = _React$useMemo2[1];

  // ======================== Modify ========================
  // When input modify content, trigger `onHelp` if is not the format
  var onModify = function onModify(text) {
    if (text && text !== format && text !== value) {
      onHelp();
    }
  };

  // ======================== Change ========================
  /**
   * Triggered by paste, keyDown and focus to show format
   */
  var triggerInputChange = (0, _rcUtil.useEvent)(function (text) {
    if (validateFormat(text)) {
      onChange(text);
    }
    setInputValue(text);
    onModify(text);
  });

  // Directly trigger `onChange` if `format` is empty
  var onInternalChange = function onInternalChange(event) {
    // Hack `onChange` with format to do nothing
    if (!format) {
      var text = event.target.value;
      onModify(text);
      setInputValue(text);
      onChange(text);
    }
  };
  var onFormatPaste = function onFormatPaste(event) {
    // Get paste text
    var pasteText = event.clipboardData.getData('text');
    if (validateFormat(pasteText)) {
      triggerInputChange(pasteText);
    }
  };

  // ======================== Mouse =========================
  // When `mouseDown` get focus, it's better to not to change the selection
  // Since the up position maybe not is the first cell
  var mouseDownRef = React.useRef(false);
  var onFormatMouseDown = function onFormatMouseDown() {
    mouseDownRef.current = true;
  };
  var onFormatMouseUp = function onFormatMouseUp(event) {
    var _ref = event.target,
      start = _ref.selectionStart;
    var closeMaskIndex = maskFormat.getMaskCellIndex(start);
    setFocusCellIndex(closeMaskIndex);

    // Force update the selection
    forceSelectionSync({});
    onMouseUp === null || onMouseUp === void 0 || onMouseUp(event);
    mouseDownRef.current = false;
  };

  // ====================== Focus Blur ======================
  var onFormatFocus = function onFormatFocus(event) {
    setFocused(true);
    setFocusCellIndex(0);
    setFocusCellText('');
    onFocus(event);
  };
  var onSharedBlur = function onSharedBlur(event) {
    onBlur(event);
  };
  var onFormatBlur = function onFormatBlur(event) {
    setFocused(false);
    onSharedBlur(event);
  };

  // ======================== Active ========================
  // Check if blur need reset input value
  (0, _useLockEffect.default)(active, function () {
    if (!active && !preserveInvalidOnBlur) {
      setInputValue(value);
    }
  });

  // ======================= Keyboard =======================
  var onSharedKeyDown = function onSharedKeyDown(event) {
    if (event.key === 'Enter' && validateFormat(inputValue)) {
      onSubmit();
    }
    onKeyDown === null || onKeyDown === void 0 || onKeyDown(event);
  };
  var onFormatKeyDown = function onFormatKeyDown(event) {
    onSharedKeyDown(event);
    var key = event.key;

    // Save the cache with cell text
    var nextCellText = null;

    // Fill in the input
    var nextFillText = null;
    var maskCellLen = selectionEnd - selectionStart;
    var cellFormat = format.slice(selectionStart, selectionEnd);

    // Cell Index
    var offsetCellIndex = function offsetCellIndex(offset) {
      setFocusCellIndex(function (idx) {
        var nextIndex = idx + offset;
        nextIndex = Math.max(nextIndex, 0);
        nextIndex = Math.min(nextIndex, maskFormat.size() - 1);
        return nextIndex;
      });
    };

    // Range
    var offsetCellValue = function offsetCellValue(offset) {
      var _getMaskRange = (0, _util.getMaskRange)(cellFormat),
        _getMaskRange2 = (0, _slicedToArray2.default)(_getMaskRange, 3),
        rangeStart = _getMaskRange2[0],
        rangeEnd = _getMaskRange2[1],
        rangeDefault = _getMaskRange2[2];
      var currentText = inputValue.slice(selectionStart, selectionEnd);
      var currentTextNum = Number(currentText);
      if (isNaN(currentTextNum)) {
        return String(rangeDefault ? rangeDefault : offset > 0 ? rangeStart : rangeEnd);
      }
      var num = currentTextNum + offset;
      var range = rangeEnd - rangeStart + 1;
      return String(rangeStart + (range + num - rangeStart) % range);
    };
    switch (key) {
      // =============== Remove ===============
      case 'Backspace':
      case 'Delete':
        nextCellText = '';
        nextFillText = cellFormat;
        break;

      // =============== Arrows ===============
      // Left key
      case 'ArrowLeft':
        nextCellText = '';
        offsetCellIndex(-1);
        break;

      // Right key
      case 'ArrowRight':
        nextCellText = '';
        offsetCellIndex(1);
        break;

      // Up key
      case 'ArrowUp':
        nextCellText = '';
        nextFillText = offsetCellValue(1);
        break;

      // Down key
      case 'ArrowDown':
        nextCellText = '';
        nextFillText = offsetCellValue(-1);
        break;

      // =============== Number ===============
      default:
        if (!isNaN(Number(key))) {
          nextCellText = focusCellText + key;
          nextFillText = nextCellText;
        }
        break;
    }

    // Update cell text
    if (nextCellText !== null) {
      setFocusCellText(nextCellText);
      if (nextCellText.length >= maskCellLen) {
        // Go to next cell
        offsetCellIndex(1);
        setFocusCellText('');
      }
    }

    // Update the input text
    if (nextFillText !== null) {
      // Replace selection range with `nextCellText`
      var nextFocusValue =
      // before
      inputValue.slice(0, selectionStart) +
      // replace
      (0, _miscUtil.leftPad)(nextFillText, maskCellLen) +
      // after
      inputValue.slice(selectionEnd);
      triggerInputChange(nextFocusValue.slice(0, format.length));
    }

    // Always trigger selection sync after key down
    forceSelectionSync({});
  };

  // ======================== Format ========================
  var rafRef = React.useRef();
  (0, _useLayoutEffect.default)(function () {
    if (!focused || !format || mouseDownRef.current) {
      return;
    }

    // Reset with format if not match
    if (!maskFormat.match(inputValue)) {
      triggerInputChange(format);
      return;
    }

    // Match the selection range
    inputRef.current.setSelectionRange(selectionStart, selectionEnd);

    // Chrome has the bug anchor position looks not correct but actually correct
    rafRef.current = (0, _raf.default)(function () {
      inputRef.current.setSelectionRange(selectionStart, selectionEnd);
    });
    return function () {
      _raf.default.cancel(rafRef.current);
    };
  }, [maskFormat, format, focused, inputValue, focusCellIndex, selectionStart, selectionEnd, forceSelectionSyncMark, triggerInputChange]);

  // ======================== Render ========================
  // Input props for format
  var inputProps = format ? {
    onFocus: onFormatFocus,
    onBlur: onFormatBlur,
    onKeyDown: onFormatKeyDown,
    onMouseDown: onFormatMouseDown,
    onMouseUp: onFormatMouseUp,
    onPaste: onFormatPaste
  } : {};
  return /*#__PURE__*/React.createElement("div", {
    ref: holderRef,
    className: (0, _classnames.default)(inputPrefixCls, (0, _defineProperty2.default)((0, _defineProperty2.default)({}, "".concat(inputPrefixCls, "-active"), active && showActiveCls), "".concat(inputPrefixCls, "-placeholder"), helped))
  }, /*#__PURE__*/React.createElement(Component, (0, _extends2.default)({
    ref: inputRef,
    "aria-invalid": invalid,
    autoComplete: "off"
  }, restProps, {
    onKeyDown: onSharedKeyDown,
    onBlur: onSharedBlur
    // Replace with format
  }, inputProps, {
    // Value
    value: inputValue,
    onChange: onInternalChange
  })), /*#__PURE__*/React.createElement(_Icon.default, {
    type: "suffix",
    icon: suffixIcon
  }), clearIcon);
});
if (process.env.NODE_ENV !== 'production') {
  Input.displayName = 'Input';
}
var _default = exports.default = Input;