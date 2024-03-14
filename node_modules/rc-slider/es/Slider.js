import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import cls from 'classnames';
import useMergedState from "rc-util/es/hooks/useMergedState";
import isEqual from "rc-util/es/isEqual";
import warning from "rc-util/es/warning";
import * as React from 'react';
import SliderContext from "./context";
import Handles from "./Handles";
import useDrag from "./hooks/useDrag";
import useOffset from "./hooks/useOffset";
import Marks from "./Marks";
import Steps from "./Steps";
import Tracks from "./Tracks";

/**
 * New:
 * - click mark to update range value
 * - handleRender
 * - Fix handle with count not correct
 * - Fix pushable not work in some case
 * - No more FindDOMNode
 * - Move all position related style into inline style
 * - Key: up is plus, down is minus
 * - fix Key with step = null not align with marks
 * - Change range should not trigger onChange
 * - keyboard support pushable
 */

var Slider = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _cls;
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-slider' : _props$prefixCls,
    className = props.className,
    style = props.style,
    classNames = props.classNames,
    styles = props.styles,
    _props$disabled = props.disabled,
    disabled = _props$disabled === void 0 ? false : _props$disabled,
    _props$keyboard = props.keyboard,
    keyboard = _props$keyboard === void 0 ? true : _props$keyboard,
    autoFocus = props.autoFocus,
    onFocus = props.onFocus,
    onBlur = props.onBlur,
    _props$min = props.min,
    min = _props$min === void 0 ? 0 : _props$min,
    _props$max = props.max,
    max = _props$max === void 0 ? 100 : _props$max,
    _props$step = props.step,
    step = _props$step === void 0 ? 1 : _props$step,
    value = props.value,
    defaultValue = props.defaultValue,
    range = props.range,
    count = props.count,
    onChange = props.onChange,
    onBeforeChange = props.onBeforeChange,
    onAfterChange = props.onAfterChange,
    onChangeComplete = props.onChangeComplete,
    _props$allowCross = props.allowCross,
    allowCross = _props$allowCross === void 0 ? true : _props$allowCross,
    _props$pushable = props.pushable,
    pushable = _props$pushable === void 0 ? false : _props$pushable,
    draggableTrack = props.draggableTrack,
    reverse = props.reverse,
    vertical = props.vertical,
    _props$included = props.included,
    included = _props$included === void 0 ? true : _props$included,
    startPoint = props.startPoint,
    trackStyle = props.trackStyle,
    handleStyle = props.handleStyle,
    railStyle = props.railStyle,
    dotStyle = props.dotStyle,
    activeDotStyle = props.activeDotStyle,
    marks = props.marks,
    dots = props.dots,
    handleRender = props.handleRender,
    _props$tabIndex = props.tabIndex,
    tabIndex = _props$tabIndex === void 0 ? 0 : _props$tabIndex,
    ariaLabelForHandle = props.ariaLabelForHandle,
    ariaLabelledByForHandle = props.ariaLabelledByForHandle,
    ariaValueTextFormatterForHandle = props.ariaValueTextFormatterForHandle;
  var handlesRef = React.useRef();
  var containerRef = React.useRef();
  var direction = React.useMemo(function () {
    if (vertical) {
      return reverse ? 'ttb' : 'btt';
    }
    return reverse ? 'rtl' : 'ltr';
  }, [reverse, vertical]);

  // ============================ Range =============================
  var mergedMin = React.useMemo(function () {
    return isFinite(min) ? min : 0;
  }, [min]);
  var mergedMax = React.useMemo(function () {
    return isFinite(max) ? max : 100;
  }, [max]);

  // ============================= Step =============================
  var mergedStep = React.useMemo(function () {
    return step !== null && step <= 0 ? 1 : step;
  }, [step]);

  // ============================= Push =============================
  var mergedPush = React.useMemo(function () {
    if (typeof pushable === 'boolean') {
      return pushable ? mergedStep : false;
    }
    return pushable >= 0 ? pushable : false;
  }, [pushable, mergedStep]);

  // ============================ Marks =============================
  var markList = React.useMemo(function () {
    var keys = Object.keys(marks || {});
    return keys.map(function (key) {
      var mark = marks[key];
      var markObj = {
        value: Number(key)
      };
      if (mark && _typeof(mark) === 'object' && ! /*#__PURE__*/React.isValidElement(mark) && ('label' in mark || 'style' in mark)) {
        markObj.style = mark.style;
        markObj.label = mark.label;
      } else {
        markObj.label = mark;
      }
      return markObj;
    }).filter(function (_ref) {
      var label = _ref.label;
      return label || typeof label === 'number';
    }).sort(function (a, b) {
      return a.value - b.value;
    });
  }, [marks]);

  // ============================ Format ============================
  var _useOffset = useOffset(mergedMin, mergedMax, mergedStep, markList, allowCross, mergedPush),
    _useOffset2 = _slicedToArray(_useOffset, 2),
    formatValue = _useOffset2[0],
    offsetValues = _useOffset2[1];

  // ============================ Values ============================
  var _useMergedState = useMergedState(defaultValue, {
      value: value
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    mergedValue = _useMergedState2[0],
    setValue = _useMergedState2[1];
  var rawValues = React.useMemo(function () {
    var valueList = mergedValue === null || mergedValue === undefined ? [] : Array.isArray(mergedValue) ? mergedValue : [mergedValue];
    var _valueList = _slicedToArray(valueList, 1),
      _valueList$ = _valueList[0],
      val0 = _valueList$ === void 0 ? mergedMin : _valueList$;
    var returnValues = mergedValue === null ? [] : [val0];

    // Format as range
    if (range) {
      returnValues = _toConsumableArray(valueList);

      // When count provided or value is `undefined`, we fill values
      if (count || mergedValue === undefined) {
        var pointCount = count >= 0 ? count + 1 : 2;
        returnValues = returnValues.slice(0, pointCount);

        // Fill with count
        while (returnValues.length < pointCount) {
          var _returnValues;
          returnValues.push((_returnValues = returnValues[returnValues.length - 1]) !== null && _returnValues !== void 0 ? _returnValues : mergedMin);
        }
      }
      returnValues.sort(function (a, b) {
        return a - b;
      });
    }

    // Align in range
    returnValues.forEach(function (val, index) {
      returnValues[index] = formatValue(val);
    });
    return returnValues;
  }, [mergedValue, range, mergedMin, count, formatValue]);

  // =========================== onChange ===========================
  var rawValuesRef = React.useRef(rawValues);
  rawValuesRef.current = rawValues;
  var getTriggerValue = function getTriggerValue(triggerValues) {
    return range ? triggerValues : triggerValues[0];
  };
  var triggerChange = function triggerChange(nextValues) {
    // Order first
    var cloneNextValues = _toConsumableArray(nextValues).sort(function (a, b) {
      return a - b;
    });

    // Trigger event if needed
    if (onChange && !isEqual(cloneNextValues, rawValuesRef.current, true)) {
      onChange(getTriggerValue(cloneNextValues));
    }

    // We set this later since it will re-render component immediately
    setValue(cloneNextValues);
  };
  var finishChange = function finishChange() {
    onAfterChange === null || onAfterChange === void 0 || onAfterChange(getTriggerValue(rawValuesRef.current));
    warning(!onAfterChange, '[rc-slider] `onAfterChange` is deprecated. Please use `onChangeComplete` instead.');
    onChangeComplete === null || onChangeComplete === void 0 || onChangeComplete(getTriggerValue(rawValuesRef.current));
  };
  var _useDrag = useDrag(containerRef, direction, rawValues, mergedMin, mergedMax, formatValue, triggerChange, finishChange, offsetValues),
    _useDrag2 = _slicedToArray(_useDrag, 4),
    draggingIndex = _useDrag2[0],
    draggingValue = _useDrag2[1],
    cacheValues = _useDrag2[2],
    onStartDrag = _useDrag2[3];
  var changeToCloseValue = function changeToCloseValue(newValue, e) {
    if (!disabled) {
      var valueIndex = 0;
      var valueDist = mergedMax - mergedMin;
      rawValues.forEach(function (val, index) {
        var dist = Math.abs(newValue - val);
        if (dist <= valueDist) {
          valueDist = dist;
          valueIndex = index;
        }
      });

      // Create new values
      var cloneNextValues = _toConsumableArray(rawValues);
      cloneNextValues[valueIndex] = newValue;

      // Fill value to match default 2
      if (range && !rawValues.length && count === undefined) {
        cloneNextValues.push(newValue);
      }
      onBeforeChange === null || onBeforeChange === void 0 || onBeforeChange(getTriggerValue(cloneNextValues));
      triggerChange(cloneNextValues);
      if (e) {
        onStartDrag(e, valueIndex, cloneNextValues);
      }
    }
  };

  // ============================ Click =============================
  var onSliderMouseDown = function onSliderMouseDown(e) {
    e.preventDefault();
    var _containerRef$current = containerRef.current.getBoundingClientRect(),
      width = _containerRef$current.width,
      height = _containerRef$current.height,
      left = _containerRef$current.left,
      top = _containerRef$current.top,
      bottom = _containerRef$current.bottom,
      right = _containerRef$current.right;
    var clientX = e.clientX,
      clientY = e.clientY;
    var percent;
    switch (direction) {
      case 'btt':
        percent = (bottom - clientY) / height;
        break;
      case 'ttb':
        percent = (clientY - top) / height;
        break;
      case 'rtl':
        percent = (right - clientX) / width;
        break;
      default:
        percent = (clientX - left) / width;
    }
    var nextValue = mergedMin + percent * (mergedMax - mergedMin);
    changeToCloseValue(formatValue(nextValue), e);
  };

  // =========================== Keyboard ===========================
  var _React$useState = React.useState(null),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    keyboardValue = _React$useState2[0],
    setKeyboardValue = _React$useState2[1];
  var onHandleOffsetChange = function onHandleOffsetChange(offset, valueIndex) {
    if (!disabled) {
      var next = offsetValues(rawValues, offset, valueIndex);
      onBeforeChange === null || onBeforeChange === void 0 || onBeforeChange(getTriggerValue(rawValues));
      triggerChange(next.values);
      setKeyboardValue(next.value);
    }
  };
  React.useEffect(function () {
    if (keyboardValue !== null) {
      var valueIndex = rawValues.indexOf(keyboardValue);
      if (valueIndex >= 0) {
        handlesRef.current.focus(valueIndex);
      }
    }
    setKeyboardValue(null);
  }, [keyboardValue]);

  // ============================= Drag =============================
  var mergedDraggableTrack = React.useMemo(function () {
    if (draggableTrack && mergedStep === null) {
      if (process.env.NODE_ENV !== 'production') {
        warning(false, '`draggableTrack` is not supported when `step` is `null`.');
      }
      return false;
    }
    return draggableTrack;
  }, [draggableTrack, mergedStep]);
  var onStartMove = function onStartMove(e, valueIndex) {
    onStartDrag(e, valueIndex);
    onBeforeChange === null || onBeforeChange === void 0 || onBeforeChange(getTriggerValue(rawValuesRef.current));
  };

  // Auto focus for updated handle
  var dragging = draggingIndex !== -1;
  React.useEffect(function () {
    if (!dragging) {
      var valueIndex = rawValues.lastIndexOf(draggingValue);
      handlesRef.current.focus(valueIndex);
    }
  }, [dragging]);

  // =========================== Included ===========================
  var sortedCacheValues = React.useMemo(function () {
    return _toConsumableArray(cacheValues).sort(function (a, b) {
      return a - b;
    });
  }, [cacheValues]);

  // Provide a range values with included [min, max]
  // Used for Track, Mark & Dot
  var _React$useMemo = React.useMemo(function () {
      if (!range) {
        return [mergedMin, sortedCacheValues[0]];
      }
      return [sortedCacheValues[0], sortedCacheValues[sortedCacheValues.length - 1]];
    }, [sortedCacheValues, range, mergedMin]),
    _React$useMemo2 = _slicedToArray(_React$useMemo, 2),
    includedStart = _React$useMemo2[0],
    includedEnd = _React$useMemo2[1];

  // ============================= Refs =============================
  React.useImperativeHandle(ref, function () {
    return {
      focus: function focus() {
        handlesRef.current.focus(0);
      },
      blur: function blur() {
        var _document = document,
          activeElement = _document.activeElement;
        if (containerRef.current.contains(activeElement)) {
          activeElement === null || activeElement === void 0 || activeElement.blur();
        }
      }
    };
  });

  // ========================== Auto Focus ==========================
  React.useEffect(function () {
    if (autoFocus) {
      handlesRef.current.focus(0);
    }
  }, []);

  // =========================== Context ============================
  var context = React.useMemo(function () {
    return {
      min: mergedMin,
      max: mergedMax,
      direction: direction,
      disabled: disabled,
      keyboard: keyboard,
      step: mergedStep,
      included: included,
      includedStart: includedStart,
      includedEnd: includedEnd,
      range: range,
      tabIndex: tabIndex,
      ariaLabelForHandle: ariaLabelForHandle,
      ariaLabelledByForHandle: ariaLabelledByForHandle,
      ariaValueTextFormatterForHandle: ariaValueTextFormatterForHandle,
      styles: styles || {},
      classNames: classNames || {}
    };
  }, [mergedMin, mergedMax, direction, disabled, keyboard, mergedStep, included, includedStart, includedEnd, range, tabIndex, ariaLabelForHandle, ariaLabelledByForHandle, ariaValueTextFormatterForHandle, styles, classNames]);

  // ============================ Render ============================
  return /*#__PURE__*/React.createElement(SliderContext.Provider, {
    value: context
  }, /*#__PURE__*/React.createElement("div", {
    ref: containerRef,
    className: cls(prefixCls, className, (_cls = {}, _defineProperty(_cls, "".concat(prefixCls, "-disabled"), disabled), _defineProperty(_cls, "".concat(prefixCls, "-vertical"), vertical), _defineProperty(_cls, "".concat(prefixCls, "-horizontal"), !vertical), _defineProperty(_cls, "".concat(prefixCls, "-with-marks"), markList.length), _cls)),
    style: style,
    onMouseDown: onSliderMouseDown
  }, /*#__PURE__*/React.createElement("div", {
    className: cls("".concat(prefixCls, "-rail"), classNames === null || classNames === void 0 ? void 0 : classNames.rail),
    style: _objectSpread(_objectSpread({}, railStyle), styles === null || styles === void 0 ? void 0 : styles.rail)
  }), /*#__PURE__*/React.createElement(Tracks, {
    prefixCls: prefixCls,
    style: trackStyle,
    values: sortedCacheValues,
    startPoint: startPoint,
    onStartMove: mergedDraggableTrack ? onStartMove : null
  }), /*#__PURE__*/React.createElement(Steps, {
    prefixCls: prefixCls,
    marks: markList,
    dots: dots,
    style: dotStyle,
    activeStyle: activeDotStyle
  }), /*#__PURE__*/React.createElement(Handles, {
    ref: handlesRef,
    prefixCls: prefixCls,
    style: handleStyle,
    values: cacheValues,
    draggingIndex: draggingIndex,
    onStartMove: onStartMove,
    onOffsetChange: onHandleOffsetChange,
    onFocus: onFocus,
    onBlur: onBlur,
    handleRender: handleRender,
    onChangeComplete: finishChange
  }), /*#__PURE__*/React.createElement(Marks, {
    prefixCls: prefixCls,
    marks: markList,
    onClick: changeToCloseValue
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Slider.displayName = 'Slider';
}
export default Slider;