"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _colors = require("../_util/colors");
var _configProvider = require("../config-provider");
var _ribbon = _interopRequireDefault(require("./style/ribbon"));
const Ribbon = props => {
  const {
    className,
    prefixCls: customizePrefixCls,
    style,
    color,
    children,
    text,
    placement = 'end',
    rootClassName
  } = props;
  const {
    getPrefixCls,
    direction
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('ribbon', customizePrefixCls);
  const wrapperCls = `${prefixCls}-wrapper`;
  const [wrapCSSVar, hashId, cssVarCls] = (0, _ribbon.default)(prefixCls, wrapperCls);
  const colorInPreset = (0, _colors.isPresetColor)(color, false);
  const ribbonCls = (0, _classnames.default)(prefixCls, `${prefixCls}-placement-${placement}`, {
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-color-${color}`]: colorInPreset
  }, className);
  const colorStyle = {};
  const cornerColorStyle = {};
  if (color && !colorInPreset) {
    colorStyle.background = color;
    cornerColorStyle.color = color;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(wrapperCls, rootClassName, hashId, cssVarCls)
  }, children, /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(ribbonCls, hashId),
    style: Object.assign(Object.assign({}, colorStyle), style)
  }, /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-text`
  }, text), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-corner`,
    style: cornerColorStyle
  }))));
};
if (process.env.NODE_ENV !== 'production') {
  Ribbon.displayName = 'Ribbon';
}
var _default = exports.default = Ribbon;