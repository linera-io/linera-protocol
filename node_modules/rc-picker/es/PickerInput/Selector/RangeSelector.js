import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["id", "clearIcon", "suffixIcon", "separator", "activeIndex", "activeHelp", "allHelp", "focused", "onFocus", "onBlur", "onKeyDown", "locale", "generateConfig", "placeholder", "className", "style", "onClick", "onClear", "value", "onChange", "onSubmit", "onInputChange", "format", "maskFormat", "preserveInvalidOnBlur", "onInvalid", "disabled", "invalid", "inputReadOnly", "direction", "onOpenChange", "onActiveOffset", "onMouseDown", "required", "aria-required", "autoFocus"],
  _excluded2 = ["index"];
import classNames from 'classnames';
import ResizeObserver from 'rc-resize-observer';
import { useEvent } from 'rc-util';
import * as React from 'react';
import PickerContext from "../context";
import useInputProps from "./hooks/useInputProps";
import useRootProps from "./hooks/useRootProps";
import Icon, { ClearIcon } from "./Icon";
import Input from "./Input";
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
    restProps = _objectWithoutProperties(props, _excluded);
  var rtl = direction === 'rtl';

  // ======================== Prefix ========================
  var _React$useContext = React.useContext(PickerContext),
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
        if (_typeof(options) === 'object') {
          var _getInput;
          var _ref = options || {},
            _ref$index = _ref.index,
            _index2 = _ref$index === void 0 ? 0 : _ref$index,
            rest = _objectWithoutProperties(_ref, _excluded2);
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
  var rootProps = useRootProps(restProps);

  // ===================== Placeholder ======================
  var mergedPlaceholder = React.useMemo(function () {
    return Array.isArray(placeholder) ? placeholder : [placeholder, placeholder];
  }, [placeholder]);

  // ======================== Inputs ========================
  var _useInputProps = useInputProps(_objectSpread(_objectSpread({}, props), {}, {
      id: ids,
      placeholder: mergedPlaceholder
    })),
    _useInputProps2 = _slicedToArray(_useInputProps, 1),
    getInputProps = _useInputProps2[0];

  // ====================== ActiveBar =======================
  var offsetUnit = rtl ? 'right' : 'left';
  var _React$useState = React.useState(_defineProperty({
      position: 'absolute',
      width: 0
    }, offsetUnit, 0)),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    activeBarStyle = _React$useState2[0],
    setActiveBarStyle = _React$useState2[1];
  var syncActiveOffset = useEvent(function () {
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
        return _objectSpread(_objectSpread({}, ori), {}, _defineProperty({
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
  return /*#__PURE__*/React.createElement(ResizeObserver, {
    onResize: syncActiveOffset
  }, /*#__PURE__*/React.createElement("div", _extends({}, rootProps, {
    className: classNames(prefixCls, "".concat(prefixCls, "-range"), _defineProperty(_defineProperty(_defineProperty(_defineProperty({}, "".concat(prefixCls, "-focused"), focused), "".concat(prefixCls, "-disabled"), disabled.every(function (i) {
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
  }), /*#__PURE__*/React.createElement(Input, _extends({
    ref: inputStartRef
  }, getInputProps(0), {
    autoFocus: startAutoFocus,
    "date-range": "start"
  })), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-range-separator")
  }, separator), /*#__PURE__*/React.createElement(Input, _extends({
    ref: inputEndRef
  }, getInputProps(1), {
    autoFocus: endAutoFocus,
    "date-range": "end"
  })), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-active-bar"),
    style: activeBarStyle
  }), /*#__PURE__*/React.createElement(Icon, {
    type: "suffix",
    icon: suffixIcon
  }), showClear && /*#__PURE__*/React.createElement(ClearIcon, {
    icon: clearIcon,
    onClear: onClear
  })));
}
var RefRangeSelector = /*#__PURE__*/React.forwardRef(RangeSelector);
if (process.env.NODE_ENV !== 'production') {
  RefRangeSelector.displayName = 'RangeSelector';
}
export default RefRangeSelector;