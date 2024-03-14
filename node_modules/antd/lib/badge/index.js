"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = _interopRequireDefault(require("rc-motion"));
var _colors = require("../_util/colors");
var _reactNode = require("../_util/reactNode");
var _configProvider = require("../config-provider");
var _Ribbon = _interopRequireDefault(require("./Ribbon"));
var _ScrollNumber = _interopRequireDefault(require("./ScrollNumber"));
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const InternalBadge = (props, ref) => {
  var _a, _b, _c, _d, _e;
  const {
      prefixCls: customizePrefixCls,
      scrollNumberPrefixCls: customizeScrollNumberPrefixCls,
      children,
      status,
      text,
      color,
      count = null,
      overflowCount = 99,
      dot = false,
      size = 'default',
      title,
      offset,
      style,
      className,
      rootClassName,
      classNames,
      styles,
      showZero = false
    } = props,
    restProps = __rest(props, ["prefixCls", "scrollNumberPrefixCls", "children", "status", "text", "color", "count", "overflowCount", "dot", "size", "title", "offset", "style", "className", "rootClassName", "classNames", "styles", "showZero"]);
  const {
    getPrefixCls,
    direction,
    badge
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('badge', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  // ================================ Misc ================================
  const numberedDisplayCount = count > overflowCount ? `${overflowCount}+` : count;
  const isZero = numberedDisplayCount === '0' || numberedDisplayCount === 0;
  const ignoreCount = count === null || isZero && !showZero;
  const hasStatus = (status !== null && status !== undefined || color !== null && color !== undefined) && ignoreCount;
  const showAsDot = dot && !isZero;
  const mergedCount = showAsDot ? '' : numberedDisplayCount;
  const isHidden = (0, _react.useMemo)(() => {
    const isEmpty = mergedCount === null || mergedCount === undefined || mergedCount === '';
    return (isEmpty || isZero && !showZero) && !showAsDot;
  }, [mergedCount, isZero, showZero, showAsDot]);
  // Count should be cache in case hidden change it
  const countRef = (0, _react.useRef)(count);
  if (!isHidden) {
    countRef.current = count;
  }
  const livingCount = countRef.current;
  // We need cache count since remove motion should not change count display
  const displayCountRef = (0, _react.useRef)(mergedCount);
  if (!isHidden) {
    displayCountRef.current = mergedCount;
  }
  const displayCount = displayCountRef.current;
  // We will cache the dot status to avoid shaking on leaved motion
  const isDotRef = (0, _react.useRef)(showAsDot);
  if (!isHidden) {
    isDotRef.current = showAsDot;
  }
  // =============================== Styles ===============================
  const mergedStyle = (0, _react.useMemo)(() => {
    if (!offset) {
      return Object.assign(Object.assign({}, badge === null || badge === void 0 ? void 0 : badge.style), style);
    }
    const offsetStyle = {
      marginTop: offset[1]
    };
    if (direction === 'rtl') {
      offsetStyle.left = parseInt(offset[0], 10);
    } else {
      offsetStyle.right = -parseInt(offset[0], 10);
    }
    return Object.assign(Object.assign(Object.assign({}, offsetStyle), badge === null || badge === void 0 ? void 0 : badge.style), style);
  }, [direction, offset, style, badge === null || badge === void 0 ? void 0 : badge.style]);
  // =============================== Render ===============================
  // >>> Title
  const titleNode = title !== null && title !== void 0 ? title : typeof livingCount === 'string' || typeof livingCount === 'number' ? livingCount : undefined;
  // >>> Status Text
  const statusTextNode = isHidden || !text ? null : /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-status-text`
  }, text);
  // >>> Display Component
  const displayNode = !livingCount || typeof livingCount !== 'object' ? undefined : (0, _reactNode.cloneElement)(livingCount, oriProps => ({
    style: Object.assign(Object.assign({}, mergedStyle), oriProps.style)
  }));
  // InternalColor
  const isInternalColor = (0, _colors.isPresetColor)(color, false);
  // Shared styles
  const statusCls = (0, _classnames.default)(classNames === null || classNames === void 0 ? void 0 : classNames.indicator, (_a = badge === null || badge === void 0 ? void 0 : badge.classNames) === null || _a === void 0 ? void 0 : _a.indicator, {
    [`${prefixCls}-status-dot`]: hasStatus,
    [`${prefixCls}-status-${status}`]: !!status,
    [`${prefixCls}-color-${color}`]: isInternalColor
  });
  const statusStyle = {};
  if (color && !isInternalColor) {
    statusStyle.color = color;
    statusStyle.background = color;
  }
  const badgeClassName = (0, _classnames.default)(prefixCls, {
    [`${prefixCls}-status`]: hasStatus,
    [`${prefixCls}-not-a-wrapper`]: !children,
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, rootClassName, badge === null || badge === void 0 ? void 0 : badge.className, (_b = badge === null || badge === void 0 ? void 0 : badge.classNames) === null || _b === void 0 ? void 0 : _b.root, classNames === null || classNames === void 0 ? void 0 : classNames.root, hashId, cssVarCls);
  // <Badge status="success" />
  if (!children && hasStatus) {
    const statusTextColor = mergedStyle.color;
    return wrapCSSVar( /*#__PURE__*/React.createElement("span", Object.assign({}, restProps, {
      className: badgeClassName,
      style: Object.assign(Object.assign(Object.assign({}, styles === null || styles === void 0 ? void 0 : styles.root), (_c = badge === null || badge === void 0 ? void 0 : badge.styles) === null || _c === void 0 ? void 0 : _c.root), mergedStyle)
    }), /*#__PURE__*/React.createElement("span", {
      className: statusCls,
      style: Object.assign(Object.assign(Object.assign({}, styles === null || styles === void 0 ? void 0 : styles.indicator), (_d = badge === null || badge === void 0 ? void 0 : badge.styles) === null || _d === void 0 ? void 0 : _d.indicator), statusStyle)
    }), text && ( /*#__PURE__*/React.createElement("span", {
      style: {
        color: statusTextColor
      },
      className: `${prefixCls}-status-text`
    }, text))));
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("span", Object.assign({
    ref: ref
  }, restProps, {
    className: badgeClassName,
    style: Object.assign(Object.assign({}, (_e = badge === null || badge === void 0 ? void 0 : badge.styles) === null || _e === void 0 ? void 0 : _e.root), styles === null || styles === void 0 ? void 0 : styles.root)
  }), children, /*#__PURE__*/React.createElement(_rcMotion.default, {
    visible: !isHidden,
    motionName: `${prefixCls}-zoom`,
    motionAppear: false,
    motionDeadline: 1000
  }, _ref => {
    let {
      className: motionClassName,
      ref: scrollNumberRef
    } = _ref;
    var _a, _b;
    const scrollNumberPrefixCls = getPrefixCls('scroll-number', customizeScrollNumberPrefixCls);
    const isDot = isDotRef.current;
    const scrollNumberCls = (0, _classnames.default)(classNames === null || classNames === void 0 ? void 0 : classNames.indicator, (_a = badge === null || badge === void 0 ? void 0 : badge.classNames) === null || _a === void 0 ? void 0 : _a.indicator, {
      [`${prefixCls}-dot`]: isDot,
      [`${prefixCls}-count`]: !isDot,
      [`${prefixCls}-count-sm`]: size === 'small',
      [`${prefixCls}-multiple-words`]: !isDot && displayCount && displayCount.toString().length > 1,
      [`${prefixCls}-status-${status}`]: !!status,
      [`${prefixCls}-color-${color}`]: isInternalColor
    });
    let scrollNumberStyle = Object.assign(Object.assign(Object.assign({}, styles === null || styles === void 0 ? void 0 : styles.indicator), (_b = badge === null || badge === void 0 ? void 0 : badge.styles) === null || _b === void 0 ? void 0 : _b.indicator), mergedStyle);
    if (color && !isInternalColor) {
      scrollNumberStyle = scrollNumberStyle || {};
      scrollNumberStyle.background = color;
    }
    return /*#__PURE__*/React.createElement(_ScrollNumber.default, {
      prefixCls: scrollNumberPrefixCls,
      show: !isHidden,
      motionClassName: motionClassName,
      className: scrollNumberCls,
      count: displayCount,
      title: titleNode,
      style: scrollNumberStyle,
      key: "scrollNumber",
      ref: scrollNumberRef
    }, displayNode);
  }), statusTextNode));
};
const Badge = /*#__PURE__*/React.forwardRef(InternalBadge);
Badge.Ribbon = _Ribbon.default;
if (process.env.NODE_ENV !== 'production') {
  Badge.displayName = 'Badge';
}
var _default = exports.default = Badge;