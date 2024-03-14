"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "TreeNode", {
  enumerable: true,
  get: function () {
    return _rcTreeSelect.TreeNode;
  }
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcTreeSelect = _interopRequireWildcard(require("rc-tree-select"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _useZIndex = require("../_util/hooks/useZIndex");
var _motion = require("../_util/motion");
var _PurePanel = _interopRequireDefault(require("../_util/PurePanel"));
var _statusUtils = require("../_util/statusUtils");
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _defaultRenderEmpty = _interopRequireDefault(require("../config-provider/defaultRenderEmpty"));
var _DisabledContext = _interopRequireDefault(require("../config-provider/DisabledContext"));
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _useSize = _interopRequireDefault(require("../config-provider/hooks/useSize"));
var _context = require("../form/context");
var _useVariants = _interopRequireDefault(require("../form/hooks/useVariants"));
var _mergedBuiltinPlacements = _interopRequireDefault(require("../select/mergedBuiltinPlacements"));
var _style = _interopRequireDefault(require("../select/style"));
var _useIcons = _interopRequireDefault(require("../select/useIcons"));
var _useShowArrow = _interopRequireDefault(require("../select/useShowArrow"));
var _Compact = require("../space/Compact");
var _iconUtil = _interopRequireDefault(require("../tree/utils/iconUtil"));
var _style2 = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const InternalTreeSelect = (_a, ref) => {
  var _b;
  var {
      prefixCls: customizePrefixCls,
      size: customizeSize,
      disabled: customDisabled,
      bordered = true,
      className,
      rootClassName,
      treeCheckable,
      multiple,
      listHeight = 256,
      listItemHeight = 26,
      placement,
      notFoundContent,
      switcherIcon,
      treeLine,
      getPopupContainer,
      popupClassName,
      dropdownClassName,
      treeIcon = false,
      transitionName,
      choiceTransitionName = '',
      status: customStatus,
      treeExpandAction,
      builtinPlacements,
      dropdownMatchSelectWidth,
      popupMatchSelectWidth,
      allowClear,
      variant: customVariant,
      dropdownStyle,
      tagRender
    } = _a,
    props = __rest(_a, ["prefixCls", "size", "disabled", "bordered", "className", "rootClassName", "treeCheckable", "multiple", "listHeight", "listItemHeight", "placement", "notFoundContent", "switcherIcon", "treeLine", "getPopupContainer", "popupClassName", "dropdownClassName", "treeIcon", "transitionName", "choiceTransitionName", "status", "treeExpandAction", "builtinPlacements", "dropdownMatchSelectWidth", "popupMatchSelectWidth", "allowClear", "variant", "dropdownStyle", "tagRender"]);
  const {
    getPopupContainer: getContextPopupContainer,
    getPrefixCls,
    renderEmpty,
    direction,
    virtual,
    popupMatchSelectWidth: contextPopupMatchSelectWidth,
    popupOverflow
  } = React.useContext(_configProvider.ConfigContext);
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('TreeSelect');
    process.env.NODE_ENV !== "production" ? warning(multiple !== false || !treeCheckable, 'usage', '`multiple` will always be `true` when `treeCheckable` is true') : void 0;
    warning.deprecated(!dropdownClassName, 'dropdownClassName', 'popupClassName');
    warning.deprecated(dropdownMatchSelectWidth === undefined, 'dropdownMatchSelectWidth', 'popupMatchSelectWidth');
    process.env.NODE_ENV !== "production" ? warning(!('showArrow' in props), 'deprecated', '`showArrow` is deprecated which will be removed in next major version. It will be a default behavior, you can hide it by setting `suffixIcon` to null.') : void 0;
    warning.deprecated(!('bordered' in props), 'bordered', 'variant');
  }
  const rootPrefixCls = getPrefixCls();
  const prefixCls = getPrefixCls('select', customizePrefixCls);
  const treePrefixCls = getPrefixCls('select-tree', customizePrefixCls);
  const treeSelectPrefixCls = getPrefixCls('tree-select', customizePrefixCls);
  const {
    compactSize,
    compactItemClassnames
  } = (0, _Compact.useCompactItemContext)(prefixCls, direction);
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const treeSelectRootCls = (0, _useCSSVarCls.default)(treeSelectPrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const [treeSelectWrapCSSVar] = (0, _style2.default)(treeSelectPrefixCls, treePrefixCls, treeSelectRootCls);
  const [variant, enableVariantCls] = (0, _useVariants.default)(customVariant, bordered);
  const mergedDropdownClassName = (0, _classnames.default)(popupClassName || dropdownClassName, `${treeSelectPrefixCls}-dropdown`, {
    [`${treeSelectPrefixCls}-dropdown-rtl`]: direction === 'rtl'
  }, rootClassName, cssVarCls, rootCls, treeSelectRootCls, hashId);
  const isMultiple = !!(treeCheckable || multiple);
  const showSuffixIcon = (0, _useShowArrow.default)(props.suffixIcon, props.showArrow);
  const mergedPopupMatchSelectWidth = (_b = popupMatchSelectWidth !== null && popupMatchSelectWidth !== void 0 ? popupMatchSelectWidth : dropdownMatchSelectWidth) !== null && _b !== void 0 ? _b : contextPopupMatchSelectWidth;
  // ===================== Form =====================
  const {
    status: contextStatus,
    hasFeedback,
    isFormItemInput,
    feedbackIcon
  } = React.useContext(_context.FormItemInputContext);
  const mergedStatus = (0, _statusUtils.getMergedStatus)(contextStatus, customStatus);
  // ===================== Icons =====================
  const {
    suffixIcon,
    removeIcon,
    clearIcon
  } = (0, _useIcons.default)(Object.assign(Object.assign({}, props), {
    multiple: isMultiple,
    showSuffixIcon,
    hasFeedback,
    feedbackIcon,
    prefixCls,
    componentName: 'TreeSelect'
  }));
  const mergedAllowClear = allowClear === true ? {
    clearIcon
  } : allowClear;
  // ===================== Empty =====================
  let mergedNotFound;
  if (notFoundContent !== undefined) {
    mergedNotFound = notFoundContent;
  } else {
    mergedNotFound = (renderEmpty === null || renderEmpty === void 0 ? void 0 : renderEmpty('Select')) || /*#__PURE__*/React.createElement(_defaultRenderEmpty.default, {
      componentName: "Select"
    });
  }
  // ==================== Render =====================
  const selectProps = (0, _omit.default)(props, ['suffixIcon', 'removeIcon', 'clearIcon', 'itemIcon', 'switcherIcon']);
  // ===================== Placement =====================
  const memoizedPlacement = React.useMemo(() => {
    if (placement !== undefined) {
      return placement;
    }
    return direction === 'rtl' ? 'bottomRight' : 'bottomLeft';
  }, [placement, direction]);
  const mergedSize = (0, _useSize.default)(ctx => {
    var _a;
    return (_a = customizeSize !== null && customizeSize !== void 0 ? customizeSize : compactSize) !== null && _a !== void 0 ? _a : ctx;
  });
  // ===================== Disabled =====================
  const disabled = React.useContext(_DisabledContext.default);
  const mergedDisabled = customDisabled !== null && customDisabled !== void 0 ? customDisabled : disabled;
  const mergedClassName = (0, _classnames.default)(!customizePrefixCls && treeSelectPrefixCls, {
    [`${prefixCls}-lg`]: mergedSize === 'large',
    [`${prefixCls}-sm`]: mergedSize === 'small',
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-${variant}`]: enableVariantCls,
    [`${prefixCls}-in-form-item`]: isFormItemInput
  }, (0, _statusUtils.getStatusClassNames)(prefixCls, mergedStatus, hasFeedback), compactItemClassnames, className, rootClassName, cssVarCls, rootCls, treeSelectRootCls, hashId);
  const renderSwitcherIcon = nodeProps => ( /*#__PURE__*/React.createElement(_iconUtil.default, {
    prefixCls: treePrefixCls,
    switcherIcon: switcherIcon,
    treeNodeProps: nodeProps,
    showLine: treeLine
  }));
  // ============================ zIndex ============================
  const [zIndex] = (0, _useZIndex.useZIndex)('SelectLike', dropdownStyle === null || dropdownStyle === void 0 ? void 0 : dropdownStyle.zIndex);
  const returnNode = /*#__PURE__*/React.createElement(_rcTreeSelect.default, Object.assign({
    virtual: virtual,
    disabled: mergedDisabled
  }, selectProps, {
    dropdownMatchSelectWidth: mergedPopupMatchSelectWidth,
    builtinPlacements: (0, _mergedBuiltinPlacements.default)(builtinPlacements, popupOverflow),
    ref: ref,
    prefixCls: prefixCls,
    className: mergedClassName,
    listHeight: listHeight,
    listItemHeight: listItemHeight,
    treeCheckable: treeCheckable ? /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-tree-checkbox-inner`
    }) : treeCheckable,
    treeLine: !!treeLine,
    suffixIcon: suffixIcon,
    multiple: isMultiple,
    placement: memoizedPlacement,
    removeIcon: removeIcon,
    allowClear: mergedAllowClear,
    switcherIcon: renderSwitcherIcon,
    showTreeIcon: treeIcon,
    notFoundContent: mergedNotFound,
    getPopupContainer: getPopupContainer || getContextPopupContainer,
    treeMotion: null,
    dropdownClassName: mergedDropdownClassName,
    dropdownStyle: Object.assign(Object.assign({}, dropdownStyle), {
      zIndex
    }),
    choiceTransitionName: (0, _motion.getTransitionName)(rootPrefixCls, '', choiceTransitionName),
    transitionName: (0, _motion.getTransitionName)(rootPrefixCls, 'slide-up', transitionName),
    treeExpandAction: treeExpandAction,
    tagRender: isMultiple ? tagRender : undefined
  }));
  return wrapCSSVar(treeSelectWrapCSSVar(returnNode));
};
const TreeSelectRef = /*#__PURE__*/React.forwardRef(InternalTreeSelect);
const TreeSelect = TreeSelectRef;
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = (0, _PurePanel.default)(TreeSelect);
TreeSelect.TreeNode = _rcTreeSelect.TreeNode;
TreeSelect.SHOW_ALL = _rcTreeSelect.SHOW_ALL;
TreeSelect.SHOW_PARENT = _rcTreeSelect.SHOW_PARENT;
TreeSelect.SHOW_CHILD = _rcTreeSelect.SHOW_CHILD;
TreeSelect._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
if (process.env.NODE_ENV !== 'production') {
  TreeSelect.displayName = 'TreeSelect';
}
var _default = exports.default = TreeSelect;