"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _gapSize = require("../_util/gapSize");
var _configProvider = require("../config-provider");
var _style = _interopRequireDefault(require("./style"));
var _utils = _interopRequireDefault(require("./utils"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Flex = /*#__PURE__*/_react.default.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      rootClassName,
      className,
      style,
      flex,
      gap,
      children,
      vertical = false,
      component: Component = 'div'
    } = props,
    othersProps = __rest(props, ["prefixCls", "rootClassName", "className", "style", "flex", "gap", "children", "vertical", "component"]);
  const {
    flex: ctxFlex,
    direction: ctxDirection,
    getPrefixCls
  } = _react.default.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('flex', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const mergedVertical = vertical !== null && vertical !== void 0 ? vertical : ctxFlex === null || ctxFlex === void 0 ? void 0 : ctxFlex.vertical;
  const mergedCls = (0, _classnames.default)(className, rootClassName, ctxFlex === null || ctxFlex === void 0 ? void 0 : ctxFlex.className, prefixCls, hashId, cssVarCls, (0, _utils.default)(prefixCls, props), {
    [`${prefixCls}-rtl`]: ctxDirection === 'rtl',
    [`${prefixCls}-gap-${gap}`]: (0, _gapSize.isPresetSize)(gap),
    [`${prefixCls}-vertical`]: mergedVertical
  });
  const mergedStyle = Object.assign(Object.assign({}, ctxFlex === null || ctxFlex === void 0 ? void 0 : ctxFlex.style), style);
  if (flex) {
    mergedStyle.flex = flex;
  }
  if (gap && !(0, _gapSize.isPresetSize)(gap)) {
    mergedStyle.gap = gap;
  }
  return wrapCSSVar( /*#__PURE__*/_react.default.createElement(Component, Object.assign({
    ref: ref,
    className: mergedCls,
    style: mergedStyle
  }, (0, _omit.default)(othersProps, ['justify', 'wrap', 'align'])), children));
});
if (process.env.NODE_ENV !== 'production') {
  Flex.displayName = 'Flex';
}
var _default = exports.default = Flex;