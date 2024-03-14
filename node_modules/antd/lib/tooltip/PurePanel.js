"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var _rcTooltip = require("rc-tooltip");
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _style = _interopRequireDefault(require("./style"));
var _util = require("./util");
/** @private Internal Component. Do not use in your production. */
const PurePanel = props => {
  const {
    prefixCls: customizePrefixCls,
    className,
    placement = 'top',
    title,
    color,
    overlayInnerStyle
  } = props;
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('tooltip', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  // Color
  const colorInfo = (0, _util.parseColor)(prefixCls, color);
  const arrowContentStyle = colorInfo.arrowStyle;
  const formattedOverlayInnerStyle = Object.assign(Object.assign({}, overlayInnerStyle), colorInfo.overlayStyle);
  const cls = (0, _classnames.default)(hashId, cssVarCls, prefixCls, `${prefixCls}-pure`, `${prefixCls}-placement-${placement}`, className, colorInfo.className);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: cls,
    style: arrowContentStyle
  }, /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-arrow`
  }), /*#__PURE__*/React.createElement(_rcTooltip.Popup, Object.assign({}, props, {
    className: hashId,
    prefixCls: prefixCls,
    overlayInnerStyle: formattedOverlayInnerStyle
  }), title)));
};
var _default = exports.default = PurePanel;