import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import { Color } from "./color";
export var ColorPickerPrefixCls = 'rc-color-picker';
export var generateColor = function generateColor(color) {
  if (color instanceof Color) {
    return color;
  }
  return new Color(color);
};
export var defaultColor = generateColor('#1677ff');
export var calculateColor = function calculateColor(props) {
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
        return generateColor(_objectSpread(_objectSpread({}, hsb), {}, {
          h: hueOffset <= 0 ? 0 : hueOffset
        }));
      case 'alpha':
        return generateColor(_objectSpread(_objectSpread({}, hsb), {}, {
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
export var calculateOffset = function calculateOffset(containerRef, targetRef, color, type) {
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