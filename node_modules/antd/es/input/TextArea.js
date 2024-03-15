"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import { forwardRef } from 'react';
import classNames from 'classnames';
import RcTextArea from 'rc-textarea';
import getAllowClear from '../_util/getAllowClear';
import { getMergedStatus, getStatusClassNames } from '../_util/statusUtils';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import DisabledContext from '../config-provider/DisabledContext';
import useCSSVarCls from '../config-provider/hooks/useCSSVarCls';
import useSize from '../config-provider/hooks/useSize';
import { FormItemInputContext } from '../form/context';
import useVariant from '../form/hooks/useVariants';
import { triggerFocus } from './Input';
import useStyle from './style';
const TextArea = /*#__PURE__*/forwardRef((props, ref) => {
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
    } = devUseWarning('TextArea');
    deprecated(!('bordered' in props), 'bordered', 'variant');
  }
  const {
    getPrefixCls,
    direction,
    textArea
  } = React.useContext(ConfigContext);
  // ===================== Size =====================
  const mergedSize = useSize(customizeSize);
  // ===================== Disabled =====================
  const disabled = React.useContext(DisabledContext);
  const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
  // ===================== Status =====================
  const {
    status: contextStatus,
    hasFeedback,
    feedbackIcon
  } = React.useContext(FormItemInputContext);
  const mergedStatus = getMergedStatus(contextStatus, customStatus);
  // ===================== Ref =====================
  const innerRef = React.useRef(null);
  React.useImperativeHandle(ref, () => {
    var _a;
    return {
      resizableTextArea: (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.resizableTextArea,
      focus: option => {
        var _a, _b;
        triggerFocus((_b = (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.resizableTextArea) === null || _b === void 0 ? void 0 : _b.textArea, option);
      },
      blur: () => {
        var _a;
        return (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.blur();
      }
    };
  });
  const prefixCls = getPrefixCls('input', customizePrefixCls);
  // ===================== Style =====================
  const rootCls = useCSSVarCls(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, rootCls);
  const [variant, enableVariantCls] = useVariant(customVariant, bordered);
  const mergedAllowClear = getAllowClear(allowClear !== null && allowClear !== void 0 ? allowClear : textArea === null || textArea === void 0 ? void 0 : textArea.allowClear);
  return wrapCSSVar( /*#__PURE__*/React.createElement(RcTextArea, Object.assign({
    autoComplete: textArea === null || textArea === void 0 ? void 0 : textArea.autoComplete
  }, rest, {
    style: Object.assign(Object.assign({}, textArea === null || textArea === void 0 ? void 0 : textArea.style), style),
    styles: Object.assign(Object.assign({}, textArea === null || textArea === void 0 ? void 0 : textArea.styles), styles),
    disabled: mergedDisabled,
    allowClear: mergedAllowClear,
    className: classNames(cssVarCls, rootCls, className, rootClassName, textArea === null || textArea === void 0 ? void 0 : textArea.className),
    classNames: Object.assign(Object.assign(Object.assign({}, classes), textArea === null || textArea === void 0 ? void 0 : textArea.classNames), {
      textarea: classNames({
        [`${prefixCls}-sm`]: mergedSize === 'small',
        [`${prefixCls}-lg`]: mergedSize === 'large'
      }, hashId, classes === null || classes === void 0 ? void 0 : classes.textarea, (_a = textArea === null || textArea === void 0 ? void 0 : textArea.classNames) === null || _a === void 0 ? void 0 : _a.textarea),
      variant: classNames({
        [`${prefixCls}-${variant}`]: enableVariantCls
      }, getStatusClassNames(prefixCls, mergedStatus)),
      affixWrapper: classNames(`${prefixCls}-textarea-affix-wrapper`, {
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
export default TextArea;