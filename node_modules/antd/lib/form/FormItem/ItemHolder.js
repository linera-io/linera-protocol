"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = ItemHolder;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _isVisible = _interopRequireDefault(require("rc-util/lib/Dom/isVisible"));
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _grid = require("../../grid");
var _context = require("../context");
var _FormItemInput = _interopRequireDefault(require("../FormItemInput"));
var _FormItemLabel = _interopRequireDefault(require("../FormItemLabel"));
var _useDebounce = _interopRequireDefault(require("../hooks/useDebounce"));
var _util = require("../util");
var _StatusProvider = _interopRequireDefault(require("./StatusProvider"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
function ItemHolder(props) {
  const {
      prefixCls,
      className,
      rootClassName,
      style,
      help,
      errors,
      warnings,
      validateStatus,
      meta,
      hasFeedback,
      hidden,
      children,
      fieldId,
      required,
      isRequired,
      onSubItemMetaChange
    } = props,
    restProps = __rest(props, ["prefixCls", "className", "rootClassName", "style", "help", "errors", "warnings", "validateStatus", "meta", "hasFeedback", "hidden", "children", "fieldId", "required", "isRequired", "onSubItemMetaChange"]);
  const itemPrefixCls = `${prefixCls}-item`;
  const {
    requiredMark
  } = React.useContext(_context.FormContext);
  // ======================== Margin ========================
  const itemRef = React.useRef(null);
  const debounceErrors = (0, _useDebounce.default)(errors);
  const debounceWarnings = (0, _useDebounce.default)(warnings);
  const hasHelp = help !== undefined && help !== null;
  const hasError = !!(hasHelp || errors.length || warnings.length);
  const isOnScreen = !!itemRef.current && (0, _isVisible.default)(itemRef.current);
  const [marginBottom, setMarginBottom] = React.useState(null);
  (0, _useLayoutEffect.default)(() => {
    if (hasError && itemRef.current) {
      // The element must be part of the DOMTree to use getComputedStyle
      // https://stackoverflow.com/questions/35360711/getcomputedstyle-returns-a-cssstyledeclaration-but-all-properties-are-empty-on-a
      const itemStyle = getComputedStyle(itemRef.current);
      setMarginBottom(parseInt(itemStyle.marginBottom, 10));
    }
  }, [hasError, isOnScreen]);
  const onErrorVisibleChanged = nextVisible => {
    if (!nextVisible) {
      setMarginBottom(null);
    }
  };
  // ======================== Status ========================
  const getValidateState = function () {
    let isDebounce = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
    const _errors = isDebounce ? debounceErrors : meta.errors;
    const _warnings = isDebounce ? debounceWarnings : meta.warnings;
    return (0, _util.getStatus)(_errors, _warnings, meta, '', !!hasFeedback, validateStatus);
  };
  const mergedValidateStatus = getValidateState();
  // ======================== Render ========================
  const itemClassName = (0, _classnames.default)(itemPrefixCls, className, rootClassName, {
    [`${itemPrefixCls}-with-help`]: hasHelp || debounceErrors.length || debounceWarnings.length,
    // Status
    [`${itemPrefixCls}-has-feedback`]: mergedValidateStatus && hasFeedback,
    [`${itemPrefixCls}-has-success`]: mergedValidateStatus === 'success',
    [`${itemPrefixCls}-has-warning`]: mergedValidateStatus === 'warning',
    [`${itemPrefixCls}-has-error`]: mergedValidateStatus === 'error',
    [`${itemPrefixCls}-is-validating`]: mergedValidateStatus === 'validating',
    [`${itemPrefixCls}-hidden`]: hidden
  });
  return /*#__PURE__*/React.createElement("div", {
    className: itemClassName,
    style: style,
    ref: itemRef
  }, /*#__PURE__*/React.createElement(_grid.Row, Object.assign({
    className: `${itemPrefixCls}-row`
  }, (0, _omit.default)(restProps, ['_internalItemRender', 'colon', 'dependencies', 'extra', 'fieldKey', 'getValueFromEvent', 'getValueProps', 'htmlFor', 'id',
  // It is deprecated because `htmlFor` is its replacement.
  'initialValue', 'isListField', 'label', 'labelAlign', 'labelCol', 'labelWrap', 'messageVariables', 'name', 'normalize', 'noStyle', 'preserve', 'requiredMark', 'rules', 'shouldUpdate', 'trigger', 'tooltip', 'validateFirst', 'validateTrigger', 'valuePropName', 'wrapperCol', 'validateDebounce'])), /*#__PURE__*/React.createElement(_FormItemLabel.default, Object.assign({
    htmlFor: fieldId
  }, props, {
    requiredMark: requiredMark,
    required: required !== null && required !== void 0 ? required : isRequired,
    prefixCls: prefixCls
  })), /*#__PURE__*/React.createElement(_FormItemInput.default, Object.assign({}, props, meta, {
    errors: debounceErrors,
    warnings: debounceWarnings,
    prefixCls: prefixCls,
    status: mergedValidateStatus,
    help: help,
    marginBottom: marginBottom,
    onErrorVisibleChanged: onErrorVisibleChanged
  }), /*#__PURE__*/React.createElement(_context.NoStyleItemContext.Provider, {
    value: onSubItemMetaChange
  }, /*#__PURE__*/React.createElement(_StatusProvider.default, {
    prefixCls: prefixCls,
    meta: meta,
    errors: meta.errors,
    warnings: meta.warnings,
    hasFeedback: hasFeedback,
    // Already calculated
    validateStatus: mergedValidateStatus
  }, children)))), !!marginBottom && ( /*#__PURE__*/React.createElement("div", {
    className: `${itemPrefixCls}-margin-offset`,
    style: {
      marginBottom: -marginBottom
    }
  })));
}