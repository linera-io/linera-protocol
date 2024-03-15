import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["prefixCls", "className", "defaultValue", "value", "count", "allowHalf", "allowClear", "character", "characterRender", "disabled", "direction", "tabIndex", "autoFocus", "onHoverChange", "onChange", "onFocus", "onBlur", "onKeyDown", "onMouseLeave"];
import classNames from 'classnames';
import useMergedState from "rc-util/es/hooks/useMergedState";
import KeyCode from "rc-util/es/KeyCode";
import pickAttrs from "rc-util/es/pickAttrs";
import React from 'react';
import Star from './Star';
import useRefs from './useRefs';
import { getOffsetLeft } from './util';
function Rate(props, ref) {
  var _classNames;
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-rate' : _props$prefixCls,
    className = props.className,
    defaultValue = props.defaultValue,
    propValue = props.value,
    _props$count = props.count,
    count = _props$count === void 0 ? 5 : _props$count,
    _props$allowHalf = props.allowHalf,
    allowHalf = _props$allowHalf === void 0 ? false : _props$allowHalf,
    _props$allowClear = props.allowClear,
    allowClear = _props$allowClear === void 0 ? true : _props$allowClear,
    _props$character = props.character,
    character = _props$character === void 0 ? '★' : _props$character,
    characterRender = props.characterRender,
    disabled = props.disabled,
    _props$direction = props.direction,
    direction = _props$direction === void 0 ? 'ltr' : _props$direction,
    _props$tabIndex = props.tabIndex,
    tabIndex = _props$tabIndex === void 0 ? 0 : _props$tabIndex,
    autoFocus = props.autoFocus,
    onHoverChange = props.onHoverChange,
    onChange = props.onChange,
    onFocus = props.onFocus,
    onBlur = props.onBlur,
    onKeyDown = props.onKeyDown,
    onMouseLeave = props.onMouseLeave,
    restProps = _objectWithoutProperties(props, _excluded);
  var _useRefs = useRefs(),
    _useRefs2 = _slicedToArray(_useRefs, 2),
    getStarRef = _useRefs2[0],
    setStarRef = _useRefs2[1];
  var rateRef = React.useRef(null);
  // ============================ Ref =============================
  var triggerFocus = function triggerFocus() {
    if (!disabled) {
      var _rateRef$current;
      (_rateRef$current = rateRef.current) === null || _rateRef$current === void 0 ? void 0 : _rateRef$current.focus();
    }
  };
  React.useImperativeHandle(ref, function () {
    return {
      focus: triggerFocus,
      blur: function blur() {
        if (!disabled) {
          var _rateRef$current2;
          (_rateRef$current2 = rateRef.current) === null || _rateRef$current2 === void 0 ? void 0 : _rateRef$current2.blur();
        }
      }
    };
  });
  // =========================== Value ============================
  var _useMergedState = useMergedState(defaultValue || 0, {
      value: propValue
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    value = _useMergedState2[0],
    setValue = _useMergedState2[1];
  var _useMergedState3 = useMergedState(null),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    cleanedValue = _useMergedState4[0],
    setCleanedValue = _useMergedState4[1];
  var getStarValue = function getStarValue(index, x) {
    var reverse = direction === 'rtl';
    var starValue = index + 1;
    if (allowHalf) {
      var starEle = getStarRef(index);
      var leftDis = getOffsetLeft(starEle);
      var width = starEle.clientWidth;
      if (reverse && x - leftDis > width / 2) {
        starValue -= 0.5;
      } else if (!reverse && x - leftDis < width / 2) {
        starValue -= 0.5;
      }
    }
    return starValue;
  };
  // >>>>> Change
  var changeValue = function changeValue(nextValue) {
    setValue(nextValue);
    onChange === null || onChange === void 0 ? void 0 : onChange(nextValue);
  };
  // =========================== Focus ============================
  var _React$useState = React.useState(false),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    focused = _React$useState2[0],
    setFocused = _React$useState2[1];
  var onInternalFocus = function onInternalFocus() {
    setFocused(true);
    onFocus === null || onFocus === void 0 ? void 0 : onFocus();
  };
  var onInternalBlur = function onInternalBlur() {
    setFocused(false);
    onBlur === null || onBlur === void 0 ? void 0 : onBlur();
  };
  // =========================== Hover ============================
  var _React$useState3 = React.useState(null),
    _React$useState4 = _slicedToArray(_React$useState3, 2),
    hoverValue = _React$useState4[0],
    setHoverValue = _React$useState4[1];
  var onHover = function onHover(event, index) {
    var nextHoverValue = getStarValue(index, event.pageX);
    if (nextHoverValue !== cleanedValue) {
      setHoverValue(nextHoverValue);
      setCleanedValue(null);
    }
    onHoverChange === null || onHoverChange === void 0 ? void 0 : onHoverChange(nextHoverValue);
  };
  var onMouseLeaveCallback = function onMouseLeaveCallback(event) {
    if (!disabled) {
      setHoverValue(null);
      setCleanedValue(null);
      onHoverChange === null || onHoverChange === void 0 ? void 0 : onHoverChange(undefined);
    }
    if (event) {
      onMouseLeave === null || onMouseLeave === void 0 ? void 0 : onMouseLeave(event);
    }
  };
  // =========================== Click ============================
  var onClick = function onClick(event, index) {
    var newValue = getStarValue(index, event.pageX);
    var isReset = false;
    if (allowClear) {
      isReset = newValue === value;
    }
    onMouseLeaveCallback();
    changeValue(isReset ? 0 : newValue);
    setCleanedValue(isReset ? newValue : null);
  };
  var onInternalKeyDown = function onInternalKeyDown(event) {
    var keyCode = event.keyCode;
    var reverse = direction === 'rtl';
    var nextValue = value;
    if (keyCode === KeyCode.RIGHT && nextValue < count && !reverse) {
      if (allowHalf) {
        nextValue += 0.5;
      } else {
        nextValue += 1;
      }
      changeValue(nextValue);
      event.preventDefault();
    } else if (keyCode === KeyCode.LEFT && nextValue > 0 && !reverse) {
      if (allowHalf) {
        nextValue -= 0.5;
      } else {
        nextValue -= 1;
      }
      changeValue(nextValue);
      event.preventDefault();
    } else if (keyCode === KeyCode.RIGHT && nextValue > 0 && reverse) {
      if (allowHalf) {
        nextValue -= 0.5;
      } else {
        nextValue -= 1;
      }
      changeValue(nextValue);
      event.preventDefault();
    } else if (keyCode === KeyCode.LEFT && nextValue < count && reverse) {
      if (allowHalf) {
        nextValue += 0.5;
      } else {
        nextValue += 1;
      }
      changeValue(nextValue);
      event.preventDefault();
    }
    onKeyDown === null || onKeyDown === void 0 ? void 0 : onKeyDown(event);
  };
  // =========================== Effect ===========================
  React.useEffect(function () {
    if (autoFocus && !disabled) {
      triggerFocus();
    }
  }, []);
  // =========================== Render ===========================
  // >>> Star
  var starNodes = new Array(count).fill(0).map(function (item, index) {
    return /*#__PURE__*/React.createElement(Star, {
      ref: setStarRef(index),
      index: index,
      count: count,
      disabled: disabled,
      prefixCls: "".concat(prefixCls, "-star"),
      allowHalf: allowHalf,
      value: hoverValue === null ? value : hoverValue,
      onClick: onClick,
      onHover: onHover,
      key: item || index,
      character: character,
      characterRender: characterRender,
      focused: focused
    });
  });
  var classString = classNames(prefixCls, className, (_classNames = {}, _defineProperty(_classNames, "".concat(prefixCls, "-disabled"), disabled), _defineProperty(_classNames, "".concat(prefixCls, "-rtl"), direction === 'rtl'), _classNames));
  // >>> Node
  return /*#__PURE__*/React.createElement("ul", _extends({
    className: classString,
    onMouseLeave: onMouseLeaveCallback,
    tabIndex: disabled ? -1 : tabIndex,
    onFocus: disabled ? null : onInternalFocus,
    onBlur: disabled ? null : onInternalBlur,
    onKeyDown: disabled ? null : onInternalKeyDown,
    ref: rateRef,
    role: "radiogroup"
  }, pickAttrs(restProps, {
    aria: true,
    data: true,
    attr: true
  })), starNodes);
}
export default /*#__PURE__*/React.forwardRef(Rate);