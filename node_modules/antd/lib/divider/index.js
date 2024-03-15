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
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Divider = props => {
  const {
    getPrefixCls,
    direction,
    divider
  } = React.useContext(_configProvider.ConfigContext);
  const {
      prefixCls: customizePrefixCls,
      type = 'horizontal',
      orientation = 'center',
      orientationMargin,
      className,
      rootClassName,
      children,
      dashed,
      plain,
      style
    } = props,
    restProps = __rest(props, ["prefixCls", "type", "orientation", "orientationMargin", "className", "rootClassName", "children", "dashed", "plain", "style"]);
  const prefixCls = getPrefixCls('divider', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const orientationPrefix = orientation.length > 0 ? `-${orientation}` : orientation;
  const hasChildren = !!children;
  const hasCustomMarginLeft = orientation === 'left' && orientationMargin != null;
  const hasCustomMarginRight = orientation === 'right' && orientationMargin != null;
  const classString = (0, _classnames.default)(prefixCls, divider === null || divider === void 0 ? void 0 : divider.className, hashId, cssVarCls, `${prefixCls}-${type}`, {
    [`${prefixCls}-with-text`]: hasChildren,
    [`${prefixCls}-with-text${orientationPrefix}`]: hasChildren,
    [`${prefixCls}-dashed`]: !!dashed,
    [`${prefixCls}-plain`]: !!plain,
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-no-default-orientation-margin-left`]: hasCustomMarginLeft,
    [`${prefixCls}-no-default-orientation-margin-right`]: hasCustomMarginRight
  }, className, rootClassName);
  const memoizedOrientationMargin = React.useMemo(() => {
    if (typeof orientationMargin === 'number') {
      return orientationMargin;
    }
    if (/^\d+$/.test(orientationMargin)) {
      return Number(orientationMargin);
    }
    return orientationMargin;
  }, [orientationMargin]);
  const innerStyle = Object.assign(Object.assign({}, hasCustomMarginLeft && {
    marginLeft: memoizedOrientationMargin
  }), hasCustomMarginRight && {
    marginRight: memoizedOrientationMargin
  });
  // Warning children not work in vertical mode
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Divider');
    process.env.NODE_ENV !== "production" ? warning(!children || type !== 'vertical', 'usage', '`children` not working in `vertical` mode.') : void 0;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({
    className: classString,
    style: Object.assign(Object.assign({}, divider === null || divider === void 0 ? void 0 : divider.style), style)
  }, restProps, {
    role: "separator"
  }), children && type !== 'vertical' && ( /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-inner-text`,
    style: innerStyle
  }, children))));
};
if (process.env.NODE_ENV !== 'production') {
  Divider.displayName = 'Divider';
}
var _default = exports.default = Divider;