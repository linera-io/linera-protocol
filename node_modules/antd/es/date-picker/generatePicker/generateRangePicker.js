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
import { forwardRef, useContext, useImperativeHandle } from 'react';
import CalendarOutlined from "@ant-design/icons/es/icons/CalendarOutlined";
import ClockCircleOutlined from "@ant-design/icons/es/icons/ClockCircleOutlined";
import SwapRightOutlined from "@ant-design/icons/es/icons/SwapRightOutlined";
import classNames from 'classnames';
import { RangePicker as RCRangePicker } from 'rc-picker';
import { useZIndex } from '../../_util/hooks/useZIndex';
import { getMergedStatus, getStatusClassNames } from '../../_util/statusUtils';
import { devUseWarning } from '../../_util/warning';
import { ConfigContext } from '../../config-provider';
import DisabledContext from '../../config-provider/DisabledContext';
import useCSSVarCls from '../../config-provider/hooks/useCSSVarCls';
import useSize from '../../config-provider/hooks/useSize';
import { FormItemInputContext } from '../../form/context';
import useVariant from '../../form/hooks/useVariants';
import { useLocale } from '../../locale';
import { NoCompactStyle, useCompactItemContext } from '../../space/Compact';
import enUS from '../locale/en_US';
import useStyle from '../style';
import { getRangePlaceholder, transPlacement2DropdownAlign, useIcons } from '../util';
import useComponents from './useComponents';
export default function generateRangePicker(generateConfig) {
  const RangePicker = /*#__PURE__*/forwardRef((props, ref) => {
    var _a;
    const {
        prefixCls: customizePrefixCls,
        getPopupContainer: customGetPopupContainer,
        components,
        className,
        style,
        placement,
        size: customizeSize,
        disabled: customDisabled,
        bordered = true,
        placeholder,
        popupClassName,
        dropdownClassName,
        status: customStatus,
        rootClassName,
        variant: customVariant
      } = props,
      restProps = __rest(props, ["prefixCls", "getPopupContainer", "components", "className", "style", "placement", "size", "disabled", "bordered", "placeholder", "popupClassName", "dropdownClassName", "status", "rootClassName", "variant"]);
    const innerRef = React.useRef(null);
    const {
      getPrefixCls,
      direction,
      getPopupContainer,
      rangePicker
    } = useContext(ConfigContext);
    const prefixCls = getPrefixCls('picker', customizePrefixCls);
    const {
      compactSize,
      compactItemClassnames
    } = useCompactItemContext(prefixCls, direction);
    const {
      picker
    } = props;
    const rootPrefixCls = getPrefixCls();
    const [variant, enableVariantCls] = useVariant(customVariant, bordered);
    const rootCls = useCSSVarCls(prefixCls);
    const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, rootCls);
    // =================== Warning =====================
    if (process.env.NODE_ENV !== 'production') {
      const warning = devUseWarning('DatePicker.RangePicker');
      warning.deprecated(!dropdownClassName, 'dropdownClassName', 'popupClassName');
      warning.deprecated(!('bordered' in props), 'bordered', 'variant');
    }
    // ===================== Icon =====================
    const [mergedAllowClear] = useIcons(props, prefixCls);
    // ================== components ==================
    const mergedComponents = useComponents(components);
    // ===================== Size =====================
    const mergedSize = useSize(ctx => {
      var _a;
      return (_a = customizeSize !== null && customizeSize !== void 0 ? customizeSize : compactSize) !== null && _a !== void 0 ? _a : ctx;
    });
    // ===================== Disabled =====================
    const disabled = React.useContext(DisabledContext);
    const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
    // ===================== FormItemInput =====================
    const formItemContext = useContext(FormItemInputContext);
    const {
      hasFeedback,
      status: contextStatus,
      feedbackIcon
    } = formItemContext;
    const suffixNode = /*#__PURE__*/React.createElement(React.Fragment, null, picker === 'time' ? /*#__PURE__*/React.createElement(ClockCircleOutlined, null) : /*#__PURE__*/React.createElement(CalendarOutlined, null), hasFeedback && feedbackIcon);
    useImperativeHandle(ref, () => innerRef.current);
    const [contextLocale] = useLocale('Calendar', enUS);
    const locale = Object.assign(Object.assign({}, contextLocale), props.locale);
    // ============================ zIndex ============================
    const [zIndex] = useZIndex('DatePicker', (_a = props.popupStyle) === null || _a === void 0 ? void 0 : _a.zIndex);
    return wrapCSSVar( /*#__PURE__*/React.createElement(NoCompactStyle, null, /*#__PURE__*/React.createElement(RCRangePicker, Object.assign({
      separator: /*#__PURE__*/React.createElement("span", {
        "aria-label": "to",
        className: `${prefixCls}-separator`
      }, /*#__PURE__*/React.createElement(SwapRightOutlined, null)),
      disabled: mergedDisabled,
      ref: innerRef,
      popupAlign: transPlacement2DropdownAlign(direction, placement),
      placeholder: getRangePlaceholder(locale, picker, placeholder),
      suffixIcon: suffixNode,
      prevIcon: /*#__PURE__*/React.createElement("span", {
        className: `${prefixCls}-prev-icon`
      }),
      nextIcon: /*#__PURE__*/React.createElement("span", {
        className: `${prefixCls}-next-icon`
      }),
      superPrevIcon: /*#__PURE__*/React.createElement("span", {
        className: `${prefixCls}-super-prev-icon`
      }),
      superNextIcon: /*#__PURE__*/React.createElement("span", {
        className: `${prefixCls}-super-next-icon`
      }),
      transitionName: `${rootPrefixCls}-slide-up`
    }, restProps, {
      className: classNames({
        [`${prefixCls}-${mergedSize}`]: mergedSize,
        [`${prefixCls}-${variant}`]: enableVariantCls
      }, getStatusClassNames(prefixCls, getMergedStatus(contextStatus, customStatus), hasFeedback), hashId, compactItemClassnames, className, rangePicker === null || rangePicker === void 0 ? void 0 : rangePicker.className, cssVarCls, rootCls, rootClassName),
      style: Object.assign(Object.assign({}, rangePicker === null || rangePicker === void 0 ? void 0 : rangePicker.style), style),
      locale: locale.lang,
      prefixCls: prefixCls,
      getPopupContainer: customGetPopupContainer || getPopupContainer,
      generateConfig: generateConfig,
      components: mergedComponents,
      direction: direction,
      classNames: {
        popup: classNames(hashId, popupClassName || dropdownClassName, cssVarCls, rootCls, rootClassName)
      },
      styles: {
        popup: Object.assign(Object.assign({}, props.popupStyle), {
          zIndex
        })
      },
      allowClear: mergedAllowClear
    }))));
  });
  if (process.env.NODE_ENV !== 'production') {
    RangePicker.displayName = 'RangePicker';
  }
  return RangePicker;
}