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
var _ref = require("rc-util/lib/ref");
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
const Typography = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      component: Component = 'article',
      className,
      rootClassName,
      setContentRef,
      children,
      direction: typographyDirection,
      style
    } = props,
    restProps = __rest(props, ["prefixCls", "component", "className", "rootClassName", "setContentRef", "children", "direction", "style"]);
  const {
    getPrefixCls,
    direction: contextDirection,
    typography
  } = React.useContext(_configProvider.ConfigContext);
  const direction = typographyDirection !== null && typographyDirection !== void 0 ? typographyDirection : contextDirection;
  let mergedRef = ref;
  if (setContentRef) {
    mergedRef = (0, _ref.composeRef)(ref, setContentRef);
  }
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Typography');
    warning.deprecated(!setContentRef, 'setContentRef', 'ref');
  }
  const prefixCls = getPrefixCls('typography', customizePrefixCls);
  // Style
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const componentClassName = (0, _classnames.default)(prefixCls, typography === null || typography === void 0 ? void 0 : typography.className, {
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, rootClassName, hashId, cssVarCls);
  const mergedStyle = Object.assign(Object.assign({}, typography === null || typography === void 0 ? void 0 : typography.style), style);
  return wrapCSSVar(
  /*#__PURE__*/
  // @ts-expect-error: Expression produces a union type that is too complex to represent.
  React.createElement(Component, Object.assign({
    className: componentClassName,
    style: mergedStyle,
    ref: mergedRef
  }, restProps), children));
});
if (process.env.NODE_ENV !== 'production') {
  Typography.displayName = 'Typography';
}
// es default export should use const instead of let
var _default = exports.default = Typography;