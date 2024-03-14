"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = _interopRequireWildcard(require("react"));
var _util = require("./util");
var _classnames = _interopRequireDefault(require("classnames"));
var _ColorBlock = _interopRequireDefault(require("./components/ColorBlock"));
var _Picker = _interopRequireDefault(require("./components/Picker"));
var _Slider = _interopRequireDefault(require("./components/Slider"));
var _useColorState3 = _interopRequireDefault(require("./hooks/useColorState"));
var hueColor = ['rgb(255, 0, 0) 0%', 'rgb(255, 255, 0) 17%', 'rgb(0, 255, 0) 33%', 'rgb(0, 255, 255) 50%', 'rgb(0, 0, 255) 67%', 'rgb(255, 0, 255) 83%', 'rgb(255, 0, 0) 100%'];
var _default = exports.default = /*#__PURE__*/(0, _react.forwardRef)(function (props, ref) {
  var value = props.value,
    defaultValue = props.defaultValue,
    _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? _util.ColorPickerPrefixCls : _props$prefixCls,
    onChange = props.onChange,
    onChangeComplete = props.onChangeComplete,
    className = props.className,
    style = props.style,
    panelRender = props.panelRender,
    _props$disabledAlpha = props.disabledAlpha,
    disabledAlpha = _props$disabledAlpha === void 0 ? false : _props$disabledAlpha,
    _props$disabled = props.disabled,
    disabled = _props$disabled === void 0 ? false : _props$disabled;
  var _useColorState = (0, _useColorState3.default)(_util.defaultColor, {
      value: value,
      defaultValue: defaultValue
    }),
    _useColorState2 = (0, _slicedToArray2.default)(_useColorState, 2),
    colorValue = _useColorState2[0],
    setColorValue = _useColorState2[1];
  var alphaColor = (0, _react.useMemo)(function () {
    var rgb = (0, _util.generateColor)(colorValue.toRgbString());
    // alpha color need equal 1 for base color
    rgb.setAlpha(1);
    return rgb.toRgbString();
  }, [colorValue]);
  var mergeCls = (0, _classnames.default)("".concat(prefixCls, "-panel"), className, (0, _defineProperty2.default)({}, "".concat(prefixCls, "-panel-disabled"), disabled));
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
  var defaultPanel = /*#__PURE__*/_react.default.createElement(_react.default.Fragment, null, /*#__PURE__*/_react.default.createElement(_Picker.default, (0, _extends2.default)({
    color: colorValue,
    onChange: handleChange
  }, basicProps)), /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-slider-container")
  }, /*#__PURE__*/_react.default.createElement("div", {
    className: (0, _classnames.default)("".concat(prefixCls, "-slider-group"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-slider-group-disabled-alpha"), disabledAlpha))
  }, /*#__PURE__*/_react.default.createElement(_Slider.default, (0, _extends2.default)({
    gradientColors: hueColor,
    color: colorValue,
    value: "hsl(".concat(colorValue.toHsb().h, ",100%, 50%)"),
    onChange: function onChange(color) {
      return handleChange(color, 'hue');
    }
  }, basicProps)), !disabledAlpha && /*#__PURE__*/_react.default.createElement(_Slider.default, (0, _extends2.default)({
    type: "alpha",
    gradientColors: ['rgba(255, 0, 4, 0) 0%', alphaColor],
    color: colorValue,
    value: colorValue.toRgbString(),
    onChange: function onChange(color) {
      return handleChange(color, 'alpha');
    }
  }, basicProps))), /*#__PURE__*/_react.default.createElement(_ColorBlock.default, {
    color: colorValue.toRgbString(),
    prefixCls: prefixCls
  })));
  return /*#__PURE__*/_react.default.createElement("div", {
    className: mergeCls,
    style: style,
    ref: ref
  }, typeof panelRender === 'function' ? panelRender(defaultPanel) : defaultPanel);
});