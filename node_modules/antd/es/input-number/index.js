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
import DownOutlined from "@ant-design/icons/es/icons/DownOutlined";
import UpOutlined from "@ant-design/icons/es/icons/UpOutlined";
import classNames from 'classnames';
import RcInputNumber from 'rc-input-number';
import { getMergedStatus, getStatusClassNames } from '../_util/statusUtils';
import { devUseWarning } from '../_util/warning';
import ConfigProvider, { ConfigContext } from '../config-provider';
import DisabledContext from '../config-provider/DisabledContext';
import useCSSVarCls from '../config-provider/hooks/useCSSVarCls';
import useSize from '../config-provider/hooks/useSize';
import { FormItemInputContext, NoFormStyle } from '../form/context';
import useVariant from '../form/hooks/useVariants';
import { NoCompactStyle, useCompactItemContext } from '../space/Compact';
import useStyle from './style';
const InputNumber = /*#__PURE__*/React.forwardRef((props, ref) => {
  if (process.env.NODE_ENV !== 'production') {
    const {
      deprecated
    } = devUseWarning('InputNumber');
    deprecated(!('bordered' in props), 'bordered', 'variant');
  }
  const {
    getPrefixCls,
    direction
  } = React.useContext(ConfigContext);
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
  const rootCls = useCSSVarCls(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, rootCls);
  const {
    compactSize,
    compactItemClassnames
  } = useCompactItemContext(prefixCls, direction);
  let upIcon = /*#__PURE__*/React.createElement(UpOutlined, {
    className: `${prefixCls}-handler-up-inner`
  });
  let downIcon = /*#__PURE__*/React.createElement(DownOutlined, {
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
  } = React.useContext(FormItemInputContext);
  const mergedStatus = getMergedStatus(contextStatus, customStatus);
  const mergedSize = useSize(ctx => {
    var _a;
    return (_a = customizeSize !== null && customizeSize !== void 0 ? customizeSize : compactSize) !== null && _a !== void 0 ? _a : ctx;
  });
  // ===================== Disabled =====================
  const disabled = React.useContext(DisabledContext);
  const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
  const [variant, enableVariantCls] = useVariant(customVariant, bordered);
  // eslint-disable-next-line react/jsx-no-useless-fragment
  const suffixNode = hasFeedback && /*#__PURE__*/React.createElement(React.Fragment, null, feedbackIcon);
  const inputNumberClass = classNames({
    [`${prefixCls}-lg`]: mergedSize === 'large',
    [`${prefixCls}-sm`]: mergedSize === 'small',
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-in-form-item`]: isFormItemInput
  }, hashId);
  const wrapperClassName = `${prefixCls}-group`;
  const element = /*#__PURE__*/React.createElement(RcInputNumber, Object.assign({
    ref: inputRef,
    disabled: mergedDisabled,
    className: classNames(cssVarCls, rootCls, className, rootClassName, compactItemClassnames),
    upHandler: upIcon,
    downHandler: downIcon,
    prefixCls: prefixCls,
    readOnly: readOnly,
    controls: controlsTemp,
    prefix: prefix,
    suffix: suffixNode,
    addonAfter: addonAfter && ( /*#__PURE__*/React.createElement(NoCompactStyle, null, /*#__PURE__*/React.createElement(NoFormStyle, {
      override: true,
      status: true
    }, addonAfter))),
    addonBefore: addonBefore && ( /*#__PURE__*/React.createElement(NoCompactStyle, null, /*#__PURE__*/React.createElement(NoFormStyle, {
      override: true,
      status: true
    }, addonBefore))),
    classNames: {
      input: inputNumberClass,
      variant: classNames({
        [`${prefixCls}-${variant}`]: enableVariantCls
      }, getStatusClassNames(prefixCls, mergedStatus, hasFeedback)),
      affixWrapper: classNames({
        [`${prefixCls}-affix-wrapper-sm`]: mergedSize === 'small',
        [`${prefixCls}-affix-wrapper-lg`]: mergedSize === 'large',
        [`${prefixCls}-affix-wrapper-rtl`]: direction === 'rtl'
      }, hashId),
      wrapper: classNames({
        [`${wrapperClassName}-rtl`]: direction === 'rtl'
      }, hashId),
      groupWrapper: classNames({
        [`${prefixCls}-group-wrapper-sm`]: mergedSize === 'small',
        [`${prefixCls}-group-wrapper-lg`]: mergedSize === 'large',
        [`${prefixCls}-group-wrapper-rtl`]: direction === 'rtl',
        [`${prefixCls}-group-wrapper-${variant}`]: enableVariantCls
      }, getStatusClassNames(`${prefixCls}-group-wrapper`, mergedStatus, hasFeedback), hashId)
    }
  }, others));
  return wrapCSSVar(element);
});
const TypedInputNumber = InputNumber;
/** @private Internal Component. Do not use in your production. */
const PureInputNumber = props => ( /*#__PURE__*/React.createElement(ConfigProvider, {
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
export default TypedInputNumber;