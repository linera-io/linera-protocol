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
var _rcTextarea = _interopRequireDefault(require("rc-textarea"));
var _getAllowClear = _interopRequireDefault(require("../_util/getAllowClear"));
var _statusUtils = require("../_util/statusUtils");
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _DisabledContext = _interopRequireDefault(require("../config-provider/DisabledContext"));
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _useSize = _interopRequireDefault(require("../config-provider/hooks/useSize"));
var _context = require("../form/context");
var _useVariants = _interopRequireDefault(require("../form/hooks/useVariants"));
var _Input = require("./Input");
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const TextArea = /*#__PURE__*/(0, _react.forwardRef)((props, ref) => {
  var _a, _b;
  const {
      prefixCls: customizePrefixCls,
      bordered = true,
      size: customizeSize,
      disabled: customDisabled,
      status: customStatus,
      allowClear,
      classNames: classes,
      rootClassName,
      className,
      style,
      styles,
      variant: customVariant
    } = props,
    rest = __rest(props, ["prefixCls", "bordered", "size", "disabled", "status", "allowClear", "classNames", "rootClassName", "className", "style", "styles", "variant"]);
  if (process.env.NODE_ENV !== 'production') {
    const {
      deprecated
    } = (0, _warning.devUseWarning)('TextArea');
    deprecated(!('bordered' in props), 'bordered', 'variant');
  }
  const {
    getPrefixCls,
    direction,
    textArea
  } = React.useContext(_configProvider.ConfigContext);
  // ===================== Size =====================
  const mergedSize = (0, _useSize.default)(customizeSize);
  // ===================== Disabled =====================
  const disabled = React.useContext(_DisabledContext.default);
  const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
  // ===================== Status =====================
  const {
    status: contextStatus,
    hasFeedback,
    feedbackIcon
  } = React.useContext(_context.FormItemInputContext);
  const mergedStatus = (0, _statusUtils.getMergedStatus)(contextStatus, customStatus);
  // ===================== Ref =====================
  const innerRef = React.useRef(null);
  React.useImperativeHandle(ref, () => {
    var _a;
    return {
      resizableTextArea: (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.resizableTextArea,
      focus: option => {
        var _a, _b;
        (0, _Input.triggerFocus)((_b = (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.resizableTextArea) === null || _b === void 0 ? void 0 : _b.textArea, option);
      },
      blur: () => {
        var _a;
        return (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.blur();
      }
    };
  });
  const prefixCls = getPrefixCls('input', customizePrefixCls);
  // ===================== Style =====================
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const [variant, enableVariantCls] = (0, _useVariants.default)(customVariant, bordered);
  const mergedAllowClear = (0, _getAllowClear.default)(allowClear !== null && allowClear !== void 0 ? allowClear : textArea === null || textArea === void 0 ? void 0 : textArea.allowClear);
  return wrapCSSVar( /*#__PURE__*/React.createElement(_rcTextarea.default, Object.assign({
    autoComplete: textArea === null || textArea === void 0 ? void 0 : textArea.autoComplete
  }, rest, {
    style: Object.assign(Object.assign({}, textArea === null || textArea === void 0 ? void 0 : textArea.style), style),
    styles: Object.assign(Object.assign({}, textArea === null || textArea === void 0 ? void 0 : textArea.styles), styles),
    disabled: mergedDisabled,
    allowClear: mergedAllowClear,
    className: (0, _classnames.default)(cssVarCls, rootCls, className, rootClassName, textArea === null || textArea === void 0 ? void 0 : textArea.className),
    classNames: Object.assign(Object.assign(Object.assign({}, classes), textArea === null || textArea === void 0 ? void 0 : textArea.classNames), {
      textarea: (0, _classnames.default)({
        [`${prefixCls}-sm`]: mergedSize === 'small',
        [`${prefixCls}-lg`]: mergedSize === 'large'
      }, hashId, classes === null || classes === void 0 ? void 0 : classes.textarea, (_a = textArea === null || textArea === void 0 ? void 0 : textArea.classNames) === null || _a === void 0 ? void 0 : _a.textarea),
      variant: (0, _classnames.default)({
        [`${prefixCls}-${variant}`]: enableVariantCls
      }, (0, _statusUtils.getStatusClassNames)(prefixCls, mergedStatus)),
      affixWrapper: (0, _classnames.default)(`${prefixCls}-textarea-affix-wrapper`, {
        [`${prefixCls}-affix-wrapper-rtl`]: direction === 'rtl',
        [`${prefixCls}-affix-wrapper-sm`]: mergedSize === 'small',
        [`${prefixCls}-affix-wrapper-lg`]: mergedSize === 'large',
        [`${prefixCls}-textarea-show-count`]: props.showCount || ((_b = props.count) === null || _b === void 0 ? void 0 : _b.show)
      }, hashId)
    }),
    prefixCls: prefixCls,
    suffix: hasFeedback && /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-textarea-suffix`
    }, feedbackIcon),
    ref: innerRef
  })));
});
var _default = exports.default = TextArea;