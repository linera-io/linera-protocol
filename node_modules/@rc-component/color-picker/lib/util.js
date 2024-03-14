"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.generateColor = exports.defaultColor = exports.calculateOffset = exports.calculateColor = exports.ColorPickerPrefixCls = void 0;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _color = require("./color");
var ColorPickerPrefixCls = exports.ColorPickerPrefixCls = 'rc-color-picker';
var generateColor = exports.generateColor = function generateColor(color) {
  if (color instanceof _color.Color) {
    return color;
  }
  return new _color.Color(color);
};
var defaultColor = exports.defaultColor = generateColor('#1677ff');
var calculateColor = exports.calculateColor = function calculateColor(props) {
  var offset = props.offset,
    targetRef = props.targetRef,
    containerRef = props.containerRef,
    color = props.color,
    type = props.type;
  var _containerRef$current = containerRef.current.getBoundingClientRect(),
    width = _containerRef$current.width,
    height = _containerRef$current.height;
  var _targetRef$current$ge = targetRef.current.getBoundingClientRect(),
    targetWidth = _targetRef$current$ge.width,
    targetHeight = _targetRef$current$ge.height;
  var centerOffsetX = targetWidth / 2;
  var centerOffsetY = targetHeight / 2;
  var saturation = (offset.x + centerOffsetX) / width;
  var bright = 1 - (offset.y + centerOffsetY) / height;
  var hsb = color.toHsb();
  var alphaOffset = saturation;
  var hueOffset = (offset.x + centerOffsetX) / width * 360;
  if (type) {
    switch (type) {
      case 'hue':
        return generateColor((0, _objectSpread2.default)((0, _objectSpread2.default)({}, hsb), {}, {
          h: hueOffset <= 0 ? 0 : hueOffset
        }));
      case 'alpha':
        return generateColor((0, _objectSpread2.default)((0, _objectSpread2.default)({}, hsb), {}, {
          a: alphaOffset <= 0 ? 0 : alphaOffset
        }));
    }
  }
  return generateColor({
    h: hsb.h,
    s: saturation <= 0 ? 0 : saturation,
    b: bright >= 1 ? 1 : bright,
    a: hsb.a
  });
};
var calculateOffset = exports.calculateOffset = function calculateOffset(containerRef, targetRef, color, type) {
  var _containerRef$current2 = containerRef.current.getBoundingClientRect(),
    width = _containerRef$current2.width,
    height = _containerRef$current2.height;
  var _targetRef$current$ge2 = targetRef.current.getBoundingClientRect(),
    targetWidth = _targetRef$current$ge2.width,
    targetHeight = _targetRef$current$ge2.height;
  var centerOffsetX = targetWidth / 2;
  var centerOffsetY = targetHeight / 2;
  var hsb = color.toHsb();

  // Exclusion of boundary cases
  if (targetWidth === 0 && targetHeight === 0 || targetWidth !== targetHeight) {
    return;
  }
  if (type) {
    switch (type) {
      case 'hue':
        return {
          x: hsb.h / 360 * width - centerOffsetX,
          y: -centerOffsetY / 3
        };
      case 'alpha':
        return {
          x: hsb.a / 1 * width - centerOffsetX,
          y: -centerOffsetY / 3
        };
    }
  }
  return {
    x: hsb.s * width - centerOffsetX,
    y: (1 - hsb.b) * height - centerOffsetY
  };
};