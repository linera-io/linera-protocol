"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _DownOutlined = _interopRequireDefault(require("@ant-design/icons/DownOutlined"));
var _UpOutlined = _interopRequireDefault(require("@ant-design/icons/UpOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcInputNumber = _interopRequireDefault(require("rc-input-number"));
var _statusUtils = require("../_util/statusUtils");
var _warning = require("../_util/warning");
var _configProvider = _interopRequireWildcard(require("../config-provider"));
var _DisabledContext = _interopRequireDefault(require("../config-provider/DisabledContext"));
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _useSize = _interopRequireDefault(require("../config-provider/hooks/useSize"));
var _context = require("../form/context");
var _useVariants = _interopRequireDefault(require("../form/hooks/useVariants"));
var _Compact = require("../space/Compact");
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const InputNumber = /*#__PURE__*/React.forwardRef((props, ref) => {
  if (process.env.NODE_ENV !== 'production') {
    const {
      deprecated
    } = (0, _warning.devUseWarning)('InputNumber');
    deprecated(!('bordered' in props), 'bordered', 'variant');
  }
  const {
    getPrefixCls,
    direction
  } = React.useContext(_configProvider.ConfigContext);
  const inputRef = React.useRef(null);
  React.useImperativeHandle(ref, () => inputRef.current);
  const {
      className,
      rootClassName,
      size: customizeSize,
      disabled: customDisabled,
      prefixCls: customizePrefixCls,
      addonBefore,
      addonAfter,
      prefix,
      bordered,
      readOnly,
      status: customStatus,
      controls,
      variant: customVariant
    } = props,
    others = __rest(props, ["className", "rootClassName", "size", "disabled", "prefixCls", "addonBefore", "addonAfter", "prefix", "bordered", "readOnly", "status", "controls", "variant"]);
  const prefixCls = getPrefixCls('input-number', customizePrefixCls);
  // Style
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const {
    compactSize,
    compactItemClassnames
  } = (0, _Compact.useCompactItemContext)(prefixCls, direction);
  let upIcon = /*#__PURE__*/React.createElement(_UpOutlined.default, {
    className: `${prefixCls}-handler-up-inner`
  });
  let downIcon = /*#__PURE__*/React.createElement(_DownOutlined.default, {
    className: `${prefixCls}-handler-down-inner`
  });
  const controlsTemp = typeof controls === 'boolean' ? controls : undefined;
  if (typeof controls === 'object') {
    upIcon = typeof controls.upIcon === 'undefined' ? upIcon : ( /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-handler-up-inner`
    }, controls.upIcon));
    downIcon = typeof controls.downIcon === 'undefined' ? downIcon : ( /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-handler-down-inner`
    }, controls.downIcon));
  }
  const {
    hasFeedback,
    status: contextStatus,
    isFormItemInput,
    feedbackIcon
  } = React.useContext(_context.FormItemInputContext);
  const mergedStatus = (0, _statusUtils.getMergedStatus)(contextStatus, customStatus);
  const mergedSize = (0, _useSize.default)(ctx => {
    var _a;
    return (_a = customizeSize !== null && customizeSize !== void 0 ? customizeSize : compactSize) !== null && _a !== void 0 ? _a : ctx;
  });
  // ===================== Disabled =====================
  const disabled = React.useContext(_DisabledContext.default);
  const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
  const [variant, enableVariantCls] = (0, _useVariants.default)(customVariant, bordered);
  // eslint-disable-next-line react/jsx-no-useless-fragment
  const suffixNode = hasFeedback && /*#__PURE__*/React.createElement(React.Fragment, null, feedbackIcon);
  const inputNumberClass = (0, _classnames.default)({
    [`${prefixCls}-lg`]: mergedSize === 'large',
    [`${prefixCls}-sm`]: mergedSize === 'small',
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-in-form-item`]: isFormItemInput
  }, hashId);
  const wrapperClassName = `${prefixCls}-group`;
  const element = /*#__PURE__*/React.createElement(_rcInputNumber.default, Object.assign({
    ref: inputRef,
    disabled: mergedDisabled,
    className: (0, _classnames.default)(cssVarCls, rootCls, className, rootClassName, compactItemClassnames),
    upHandler: upIcon,
    downHandler: downIcon,
    prefixCls: prefixCls,
    readOnly: readOnly,
    controls: controlsTemp,
    prefix: prefix,
    suffix: suffixNode,
    addonAfter: addonAfter && ( /*#__PURE__*/React.createElement(_Compact.NoCompactStyle, null, /*#__PURE__*/React.createElement(_context.NoFormStyle, {
      override: true,
      status: true
    }, addonAfter))),
    addonBefore: addonBefore && ( /*#__PURE__*/React.createElement(_Compact.NoCompactStyle, null, /*#__PURE__*/React.createElement(_context.NoFormStyle, {
      override: true,
      status: true
    }, addonBefore))),
    classNames: {
      input: inputNumberClass,
      variant: (0, _classnames.default)({
        [`${prefixCls}-${variant}`]: enableVariantCls
      }, (0, _statusUtils.getStatusClassNames)(prefixCls, mergedStatus, hasFeedback)),
      affixWrapper: (0, _classnames.default)({
        [`${prefixCls}-affix-wrapper-sm`]: mergedSize === 'small',
        [`${prefixCls}-affix-wrapper-lg`]: mergedSize === 'large',
        [`${prefixCls}-affix-wrapper-rtl`]: direction === 'rtl'
      }, hashId),
      wrapper: (0, _classnames.default)({
        [`${wrapperClassName}-rtl`]: direction === 'rtl'
      }, hashId),
      groupWrapper: (0, _classnames.default)({
        [`${prefixCls}-group-wrapper-sm`]: mergedSize === 'small',
        [`${prefixCls}-group-wrapper-lg`]: mergedSize === 'large',
        [`${prefixCls}-group-wrapper-rtl`]: direction === 'rtl',
        [`${prefixCls}-group-wrapper-${variant}`]: enableVariantCls
      }, (0, _statusUtils.getStatusClassNames)(`${prefixCls}-group-wrapper`, mergedStatus, hasFeedback), hashId)
    }
  }, others));
  return wrapCSSVar(element);
});
const TypedInputNumber = InputNumber;
/** @private Internal Component. Do not use in your production. */
const PureInputNumber = props => ( /*#__PURE__*/React.createElement(_configProvider.default, {
  theme: {
    components: {
      InputNumber: {
        handleVisible: true
      }
    }
  }
}, /*#__PURE__*/React.createElement(InputNumber, Object.assign({}, props))));
if (process.env.NODE_ENV !== 'production') {
  TypedInputNumber.displayName = 'InputNumber';
}
TypedInputNumber._InternalPanelDoNotUseOrYouWillBeFired = PureInputNumber;
var _default = exports.default = TypedInputNumber;