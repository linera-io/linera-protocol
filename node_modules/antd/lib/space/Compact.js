"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useCompactItemContext = exports.default = exports.SpaceCompactItemContext = exports.NoCompactStyle = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _useSize = _interopRequireDefault(require("../config-provider/hooks/useSize"));
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const SpaceCompactItemContext = exports.SpaceCompactItemContext = /*#__PURE__*/React.createContext(null);
const useCompactItemContext = (prefixCls, direction) => {
  const compactItemContext = React.useContext(SpaceCompactItemContext);
  const compactItemClassnames = React.useMemo(() => {
    if (!compactItemContext) {
      return '';
    }
    const {
      compactDirection,
      isFirstItem,
      isLastItem
    } = compactItemContext;
    const separator = compactDirection === 'vertical' ? '-vertical-' : '-';
    return (0, _classnames.default)(`${prefixCls}-compact${separator}item`, {
      [`${prefixCls}-compact${separator}first-item`]: isFirstItem,
      [`${prefixCls}-compact${separator}last-item`]: isLastItem,
      [`${prefixCls}-compact${separator}item-rtl`]: direction === 'rtl'
    });
  }, [prefixCls, direction, compactItemContext]);
  return {
    compactSize: compactItemContext === null || compactItemContext === void 0 ? void 0 : compactItemContext.compactSize,
    compactDirection: compactItemContext === null || compactItemContext === void 0 ? void 0 : compactItemContext.compactDirection,
    compactItemClassnames
  };
};
exports.useCompactItemContext = useCompactItemContext;
const NoCompactStyle = _ref => {
  let {
    children
  } = _ref;
  return /*#__PURE__*/React.createElement(SpaceCompactItemContext.Provider, {
    value: null
  }, children);
};
exports.NoCompactStyle = NoCompactStyle;
const CompactItem = _a => {
  var {
      children
    } = _a,
    otherProps = __rest(_a, ["children"]);
  return /*#__PURE__*/React.createElement(SpaceCompactItemContext.Provider, {
    value: otherProps
  }, children);
};
const Compact = props => {
  const {
    getPrefixCls,
    direction: directionConfig
  } = React.useContext(_configProvider.ConfigContext);
  const {
      size,
      direction,
      block,
      prefixCls: customizePrefixCls,
      className,
      rootClassName,
      children
    } = props,
    restProps = __rest(props, ["size", "direction", "block", "prefixCls", "className", "rootClassName", "children"]);
  const mergedSize = (0, _useSize.default)(ctx => size !== null && size !== void 0 ? size : ctx);
  const prefixCls = getPrefixCls('space-compact', customizePrefixCls);
  const [wrapCSSVar, hashId] = (0, _style.default)(prefixCls);
  const clx = (0, _classnames.default)(prefixCls, hashId, {
    [`${prefixCls}-rtl`]: directionConfig === 'rtl',
    [`${prefixCls}-block`]: block,
    [`${prefixCls}-vertical`]: direction === 'vertical'
  }, className, rootClassName);
  const compactItemContext = React.useContext(SpaceCompactItemContext);
  const childNodes = (0, _toArray.default)(children);
  const nodes = React.useMemo(() => childNodes.map((child, i) => {
    const key = child && child.key || `${prefixCls}-item-${i}`;
    return /*#__PURE__*/React.createElement(CompactItem, {
      key: key,
      compactSize: mergedSize,
      compactDirection: direction,
      isFirstItem: i === 0 && (!compactItemContext || (compactItemContext === null || compactItemContext === void 0 ? void 0 : compactItemContext.isFirstItem)),
      isLastItem: i === childNodes.length - 1 && (!compactItemContext || (compactItemContext === null || compactItemContext === void 0 ? void 0 : compactItemContext.isLastItem))
    }, child);
  }), [size, childNodes, compactItemContext]);
  // =========================== Render ===========================
  if (childNodes.length === 0) {
    return null;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({
    className: clx
  }, restProps), nodes));
};
var _default = exports.default = Compact;