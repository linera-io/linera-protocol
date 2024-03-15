import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import React, { forwardRef, useMemo } from 'react';
import { ColorPickerPrefixCls, defaultColor, generateColor } from "./util";
import classNames from 'classnames';
import ColorBlock from "./components/ColorBlock";
import Picker from "./components/Picker";
import Slider from "./components/Slider";
import useColorState from "./hooks/useColorState";
var hueColor = ['rgb(255, 0, 0) 0%', 'rgb(255, 255, 0) 17%', 'rgb(0, 255, 0) 33%', 'rgb(0, 255, 255) 50%', 'rgb(0, 0, 255) 67%', 'rgb(255, 0, 255) 83%', 'rgb(255, 0, 0) 100%'];
export default /*#__PURE__*/forwardRef(function (props, ref) {
  var value = props.value,
    defaultValue = props.defaultValue,
    _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? ColorPickerPrefixCls : _props$prefixCls,
    onChange = props.onChange,
    onChangeComplete = props.onChangeComplete,
    className = props.className,
    style = props.style,
    panelRender = props.panelRender,
    _props$disabledAlpha = props.disabledAlpha,
    disabledAlpha = _props$disabledAlpha === void 0 ? false : _props$disabledAlpha,
    _props$disabled = props.disabled,
    disabled = _props$disabled === void 0 ? false : _props$disabled;
  var _useColorState = useColorState(defaultColor, {
      value: value,
      defaultValue: defaultValue
    }),
    _useColorState2 = _slicedToArray(_useColorState, 2),
    colorValue = _useColorState2[0],
    setColorValue = _useColorState2[1];
  var alphaColor = useMemo(function () {
    var rgb = generateColor(colorValue.toRgbString());
    // alpha color need equal 1 for base color
    rgb.setAlpha(1);
    return rgb.toRgbString();
  }, [colorValue]);
  var mergeCls = classNames("".concat(prefixCls, "-panel"), className, _defineProperty({}, "".concat(prefixCls, "-panel-disabled"), disabled));
  var basicProps = {
    prefixCls: prefixCls,
    onChangeComplete: onChangeComplete,
    disabled: disabled
  };
  var handleChange = function handleChange(data, type) {
    if (!value) {
      setColorValue(data);
    }
    onChange === null || onChange === void 0 || onChange(data, type);
  };
  var defaultPanel = /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(Picker, _extends({
    color: colorValue,
    onChange: handleChange
  }, basicProps)), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-slider-container")
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames("".concat(prefixCls, "-slider-group"), _defineProperty({}, "".concat(prefixCls, "-slider-group-disabled-alpha"), disabledAlpha))
  }, /*#__PURE__*/React.createElement(Slider, _extends({
    gradientColors: hueColor,
    color: colorValue,
    value: "hsl(".concat(colorValue.toHsb().h, ",100%, 50%)"),
    onChange: function onChange(color) {
      return handleChange(color, 'hue');
    }
  }, basicProps)), !disabledAlpha && /*#__PURE__*/React.createElement(Slider, _extends({
    type: "alpha",
    gradientColors: ['rgba(255, 0, 4, 0) 0%', alphaColor],
    color: colorValue,
    value: colorValue.toRgbString(),
    onChange: function onChange(color) {
      return handleChange(color, 'alpha');
    }
  }, basicProps))), /*#__PURE__*/React.createElement(ColorBlock, {
    color: colorValue.toRgbString(),
    prefixCls: prefixCls
  })));
  return /*#__PURE__*/React.createElement("div", {
    className: mergeCls,
    style: style,
    ref: ref
  }, typeof panelRender === 'function' ? panelRender(defaultPanel) : defaultPanel);
});