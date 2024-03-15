"use client";

import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import classNames from 'classnames';
import RcCascader from 'rc-cascader';
import omit from "rc-util/es/omit";
import { useZIndex } from '../_util/hooks/useZIndex';
import { getTransitionName } from '../_util/motion';
import genPurePanel from '../_util/PurePanel';
import { getMergedStatus, getStatusClassNames } from '../_util/statusUtils';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import DefaultRenderEmpty from '../config-provider/defaultRenderEmpty';
import DisabledContext from '../config-provider/DisabledContext';
import useCSSVarCls from '../config-provider/hooks/useCSSVarCls';
import useSize from '../config-provider/hooks/useSize';
import { FormItemInputContext } from '../form/context';
import useVariant from '../form/hooks/useVariants';
import mergedBuiltinPlacements from '../select/mergedBuiltinPlacements';
import useSelectStyle from '../select/style';
import useIcons from '../select/useIcons';
import useShowArrow from '../select/useShowArrow';
import { useCompactItemContext } from '../space/Compact';
import useBase from './hooks/useBase';
import useCheckable from './hooks/useCheckable';
import useColumnIcons from './hooks/useColumnIcons';
import CascaderPanel from './Panel';
import useStyle from './style';
const {
  SHOW_CHILD,
  SHOW_PARENT
} = RcCascader;
function highlightKeyword(str, lowerKeyword, prefixCls) {
  const cells = str.toLowerCase().split(lowerKeyword).reduce((list, cur, index) => index === 0 ? [cur] : [].concat(_toConsumableArray(list), [lowerKeyword, cur]), []);
  const fillCells = [];
  let start = 0;
  cells.forEach((cell, index) => {
    const end = start + cell.length;
    let originWorld = str.slice(start, end);
    start = end;
    if (index % 2 === 1) {
      originWorld =
      /*#__PURE__*/
      // eslint-disable-next-line react/no-array-index-key
      React.createElement("span", {
        className: `${prefixCls}-menu-item-keyword`,
        key: `separator-${index}`
      }, originWorld);
    }
    fillCells.push(originWorld);
  });
  return fillCells;
}
const defaultSearchRender = (inputValue, path, prefixCls, fieldNames) => {
  const optionList = [];
  // We do lower here to save perf
  const lower = inputValue.toLowerCase();
  path.forEach((node, index) => {
    if (index !== 0) {
      optionList.push(' / ');
    }
    let label = node[fieldNames.label];
    const type = typeof label;
    if (type === 'string' || type === 'number') {
      label = highlightKeyword(String(label), lower, prefixCls);
    }
    optionList.push(label);
  });
  return optionList;
};
const Cascader = /*#__PURE__*/React.forwardRef((props, ref) => {
  var _a;
  const {
      prefixCls: customizePrefixCls,
      size: customizeSize,
      disabled: customDisabled,
      className,
      rootClassName,
      multiple,
      bordered = true,
      transitionName,
      choiceTransitionName = '',
      popupClassName,
      dropdownClassName,
      expandIcon,
      placement,
      showSearch,
      allowClear = true,
      notFoundContent,
      direction,
      getPopupContainer,
      status: customStatus,
      showArrow,
      builtinPlacements,
      style,
      variant: customVariant
    } = props,
    rest = __rest(props, ["prefixCls", "size", "disabled", "className", "rootClassName", "multiple", "bordered", "transitionName", "choiceTransitionName", "popupClassName", "dropdownClassName", "expandIcon", "placement", "showSearch", "allowClear", "notFoundContent", "direction", "getPopupContainer", "status", "showArrow", "builtinPlacements", "style", "variant"]);
  const restProps = omit(rest, ['suffixIcon']);
  const {
    getPopupContainer: getContextPopupContainer,
    getPrefixCls,
    popupOverflow,
    cascader
  } = React.useContext(ConfigContext);
  // =================== Form =====================
  const {
    status: contextStatus,
    hasFeedback,
    isFormItemInput,
    feedbackIcon
  } = React.useContext(FormItemInputContext);
  const mergedStatus = getMergedStatus(contextStatus, customStatus);
  // =================== Warning =====================
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Cascader');
    warning.deprecated(!dropdownClassName, 'dropdownClassName', 'popupClassName');
    process.env.NODE_ENV !== "production" ? warning(!('showArrow' in props), 'deprecated', '`showArrow` is deprecated which will be removed in next major version. It will be a default behavior, you can hide it by setting `suffixIcon` to null.') : void 0;
    warning.deprecated(!('bordered' in props), 'bordered', 'variant');
  }
  // ==================== Prefix =====================
  const [prefixCls, cascaderPrefixCls, mergedDirection, renderEmpty] = useBase(customizePrefixCls, direction);
  const isRtl = mergedDirection === 'rtl';
  const rootPrefixCls = getPrefixCls();
  const rootCls = useCSSVarCls(prefixCls);
  const [wrapSelectCSSVar, hashId, cssVarCls] = useSelectStyle(prefixCls, rootCls);
  const cascaderRootCls = useCSSVarCls(cascaderPrefixCls);
  const [wrapCascaderCSSVar] = useStyle(cascaderPrefixCls, cascaderRootCls);
  const {
    compactSize,
    compactItemClassnames
  } = useCompactItemContext(prefixCls, direction);
  const [variant, enableVariantCls] = useVariant(customVariant, bordered);
  // =================== No Found ====================
  const mergedNotFoundContent = notFoundContent || (renderEmpty === null || renderEmpty === void 0 ? void 0 : renderEmpty('Cascader')) || ( /*#__PURE__*/React.createElement(DefaultRenderEmpty, {
    componentName: "Cascader"
  }));
  // =================== Dropdown ====================
  const mergedDropdownClassName = classNames(popupClassName || dropdownClassName, `${cascaderPrefixCls}-dropdown`, {
    [`${cascaderPrefixCls}-dropdown-rtl`]: mergedDirection === 'rtl'
  }, rootClassName, rootCls, cascaderRootCls, hashId, cssVarCls);
  // ==================== Search =====================
  const mergedShowSearch = React.useMemo(() => {
    if (!showSearch) {
      return showSearch;
    }
    let searchConfig = {
      render: defaultSearchRender
    };
    if (typeof showSearch === 'object') {
      searchConfig = Object.assign(Object.assign({}, searchConfig), showSearch);
    }
    return searchConfig;
  }, [showSearch]);
  // ===================== Size ======================
  const mergedSize = useSize(ctx => {
    var _a;
    return (_a = customizeSize !== null && customizeSize !== void 0 ? customizeSize : compactSize) !== null && _a !== void 0 ? _a : ctx;
  });
  // ===================== Disabled =====================
  const disabled = React.useContext(DisabledContext);
  const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
  // ===================== Icon ======================
  const [mergedExpandIcon, loadingIcon] = useColumnIcons(prefixCls, isRtl, expandIcon);
  // =================== Multiple ====================
  const checkable = useCheckable(cascaderPrefixCls, multiple);
  // ===================== Icons =====================
  const showSuffixIcon = useShowArrow(props.suffixIcon, showArrow);
  const {
    suffixIcon,
    removeIcon,
    clearIcon
  } = useIcons(Object.assign(Object.assign({}, props), {
    hasFeedback,
    feedbackIcon,
    showSuffixIcon,
    multiple,
    prefixCls,
    componentName: 'Cascader'
  }));
  // ===================== Placement =====================
  const memoPlacement = React.useMemo(() => {
    if (placement !== undefined) {
      return placement;
    }
    return isRtl ? 'bottomRight' : 'bottomLeft';
  }, [placement, isRtl]);
  const mergedAllowClear = allowClear === true ? {
    clearIcon
  } : allowClear;
  // ============================ zIndex ============================
  const [zIndex] = useZIndex('SelectLike', (_a = restProps.dropdownStyle) === null || _a === void 0 ? void 0 : _a.zIndex);
  // ==================== Render =====================
  const renderNode = /*#__PURE__*/React.createElement(RcCascader, Object.assign({
    prefixCls: prefixCls,
    className: classNames(!customizePrefixCls && cascaderPrefixCls, {
      [`${prefixCls}-lg`]: mergedSize === 'large',
      [`${prefixCls}-sm`]: mergedSize === 'small',
      [`${prefixCls}-rtl`]: isRtl,
      [`${prefixCls}-${variant}`]: enableVariantCls,
      [`${prefixCls}-in-form-item`]: isFormItemInput
    }, getStatusClassNames(prefixCls, mergedStatus, hasFeedback), compactItemClassnames, cascader === null || cascader === void 0 ? void 0 : cascader.className, className, rootClassName, rootCls, cascaderRootCls, hashId, cssVarCls),
    disabled: mergedDisabled,
    style: Object.assign(Object.assign({}, cascader === null || cascader === void 0 ? void 0 : cascader.style), style)
  }, restProps, {
    builtinPlacements: mergedBuiltinPlacements(builtinPlacements, popupOverflow),
    direction: mergedDirection,
    placement: memoPlacement,
    notFoundContent: mergedNotFoundContent,
    allowClear: mergedAllowClear,
    showSearch: mergedShowSearch,
    expandIcon: mergedExpandIcon,
    suffixIcon: suffixIcon,
    removeIcon: removeIcon,
    loadingIcon: loadingIcon,
    checkable: checkable,
    dropdownClassName: mergedDropdownClassName,
    dropdownPrefixCls: customizePrefixCls || cascaderPrefixCls,
    dropdownStyle: Object.assign(Object.assign({}, restProps.dropdownStyle), {
      zIndex
    }),
    choiceTransitionName: getTransitionName(rootPrefixCls, '', choiceTransitionName),
    transitionName: getTransitionName(rootPrefixCls, 'slide-up', transitionName),
    getPopupContainer: getPopupContainer || getContextPopupContainer,
    ref: ref
  }));
  return wrapCascaderCSSVar(wrapSelectCSSVar(renderNode));
});
if (process.env.NODE_ENV !== 'production') {
  Cascader.displayName = 'Cascader';
}
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = genPurePanel(Cascader);
Cascader.SHOW_PARENT = SHOW_PARENT;
Cascader.SHOW_CHILD = SHOW_CHILD;
Cascader.Panel = CascaderPanel;
Cascader._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
export default Cascader;