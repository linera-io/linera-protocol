"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.parseColor = parseColor;
var _classnames = _interopRequireDefault(require("classnames"));
var _colors = require("../_util/colors");
/* eslint-disable import/prefer-default-export */

function parseColor(prefixCls, color) {
  const isInternalColor = (0, _colors.isPresetColor)(color);
  const className = (0, _classnames.default)({
    [`${prefixCls}-${color}`]: color && isInternalColor
  });
  const overlayStyle = {};
  const arrowStyle = {};
  if (color && !isInternalColor) {
    overlayStyle.background = color;
    // @ts-ignore
    arrowStyle['--antd-arrow-background-color'] = color;
  }
  return {
    className,
    overlayStyle,
    arrowStyle
  };
}