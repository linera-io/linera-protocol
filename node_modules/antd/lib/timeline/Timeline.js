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
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _TimelineItem = _interopRequireDefault(require("./TimelineItem"));
var _TimelineItemList = _interopRequireDefault(require("./TimelineItemList"));
var _useItems = _interopRequireDefault(require("./useItems"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};

// CSSINJS

const Timeline = props => {
  const {
    getPrefixCls,
    direction,
    timeline
  } = React.useContext(_configProvider.ConfigContext);
  const {
      prefixCls: customizePrefixCls,
      children,
      items,
      className,
      style
    } = props,
    restProps = __rest(props, ["prefixCls", "children", "items", "className", "style"]);
  const prefixCls = getPrefixCls('timeline', customizePrefixCls);
  // =================== Warning =====================
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Timeline');
    warning.deprecated(!children, 'Timeline.Item', 'items');
  }
  // Style
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const mergedItems = (0, _useItems.default)(items, children);
  return wrapCSSVar( /*#__PURE__*/React.createElement(_TimelineItemList.default, Object.assign({}, restProps, {
    className: (0, _classnames.default)(timeline === null || timeline === void 0 ? void 0 : timeline.className, className, cssVarCls, rootCls),
    style: Object.assign(Object.assign({}, timeline === null || timeline === void 0 ? void 0 : timeline.style), style),
    prefixCls: prefixCls,
    direction: direction,
    items: mergedItems,
    hashId: hashId
  })));
};
Timeline.Item = _TimelineItem.default;
if (process.env.NODE_ENV !== 'production') {
  Timeline.displayName = 'Timeline';
}
var _default = exports.default = Timeline;