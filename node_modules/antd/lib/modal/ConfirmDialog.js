"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ConfirmContent = ConfirmContent;
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var React = _interopRequireWildcard(require("react"));
var _CheckCircleFilled = _interopRequireDefault(require("@ant-design/icons/CheckCircleFilled"));
var _CloseCircleFilled = _interopRequireDefault(require("@ant-design/icons/CloseCircleFilled"));
var _ExclamationCircleFilled = _interopRequireDefault(require("@ant-design/icons/ExclamationCircleFilled"));
var _InfoCircleFilled = _interopRequireDefault(require("@ant-design/icons/InfoCircleFilled"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useZIndex = require("../_util/hooks/useZIndex");
var _motion = require("../_util/motion");
var _warning = require("../_util/warning");
var _configProvider = _interopRequireDefault(require("../config-provider"));
var _locale = require("../locale");
var _useToken = _interopRequireDefault(require("../theme/useToken"));
var _ConfirmCancelBtn = _interopRequireDefault(require("./components/ConfirmCancelBtn"));
var _ConfirmOkBtn = _interopRequireDefault(require("./components/ConfirmOkBtn"));
var _context = require("./context");
var _Modal = _interopRequireDefault(require("./Modal"));
var _confirm = _interopRequireDefault(require("./style/confirm"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
function ConfirmContent(props) {
  const {
      prefixCls,
      icon,
      okText,
      cancelText,
      confirmPrefixCls,
      type,
      okCancel,
      footer,
      // Legacy for static function usage
      locale: staticLocale
    } = props,
    resetProps = __rest(props, ["prefixCls", "icon", "okText", "cancelText", "confirmPrefixCls", "type", "okCancel", "footer", "locale"]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Modal');
    process.env.NODE_ENV !== "production" ? warning(!(typeof icon === 'string' && icon.length > 2), 'breaking', `\`icon\` is using ReactNode instead of string naming in v4. Please check \`${icon}\` at https://ant.design/components/icon`) : void 0;
  }
  // Icon
  let mergedIcon = icon;
  // 支持传入{ icon: null }来隐藏`Modal.confirm`默认的Icon
  if (!icon && icon !== null) {
    switch (type) {
      case 'info':
        mergedIcon = /*#__PURE__*/React.createElement(_InfoCircleFilled.default, null);
        break;
      case 'success':
        mergedIcon = /*#__PURE__*/React.createElement(_CheckCircleFilled.default, null);
        break;
      case 'error':
        mergedIcon = /*#__PURE__*/React.createElement(_CloseCircleFilled.default, null);
        break;
      default:
        mergedIcon = /*#__PURE__*/React.createElement(_ExclamationCircleFilled.default, null);
    }
  }
  // 默认为 true，保持向下兼容
  const mergedOkCancel = okCancel !== null && okCancel !== void 0 ? okCancel : type === 'confirm';
  const autoFocusButton = props.autoFocusButton === null ? false : props.autoFocusButton || 'ok';
  const [locale] = (0, _locale.useLocale)('Modal');
  const mergedLocale = staticLocale || locale;
  // ================== Locale Text ==================
  const okTextLocale = okText || (mergedOkCancel ? mergedLocale === null || mergedLocale === void 0 ? void 0 : mergedLocale.okText : mergedLocale === null || mergedLocale === void 0 ? void 0 : mergedLocale.justOkText);
  const cancelTextLocale = cancelText || (mergedLocale === null || mergedLocale === void 0 ? void 0 : mergedLocale.cancelText);
  // ================= Context Value =================
  const btnCtxValue = Object.assign({
    autoFocusButton,
    cancelTextLocale,
    okTextLocale,
    mergedOkCancel
  }, resetProps);
  const btnCtxValueMemo = React.useMemo(() => btnCtxValue, (0, _toConsumableArray2.default)(Object.values(btnCtxValue)));
  // ====================== Footer Origin Node ======================
  const footerOriginNode = /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(_ConfirmCancelBtn.default, null), /*#__PURE__*/React.createElement(_ConfirmOkBtn.default, null));
  const hasTitle = props.title !== undefined && props.title !== null;
  const bodyCls = `${confirmPrefixCls}-body`;
  return /*#__PURE__*/React.createElement("div", {
    className: `${confirmPrefixCls}-body-wrapper`
  }, /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(bodyCls, {
      [`${bodyCls}-has-title`]: hasTitle
    })
  }, mergedIcon, /*#__PURE__*/React.createElement("div", {
    className: `${confirmPrefixCls}-paragraph`
  }, hasTitle && /*#__PURE__*/React.createElement("span", {
    className: `${confirmPrefixCls}-title`
  }, props.title), /*#__PURE__*/React.createElement("div", {
    className: `${confirmPrefixCls}-content`
  }, props.content))), footer === undefined || typeof footer === 'function' ? ( /*#__PURE__*/React.createElement(_context.ModalContextProvider, {
    value: btnCtxValueMemo
  }, /*#__PURE__*/React.createElement("div", {
    className: `${confirmPrefixCls}-btns`
  }, typeof footer === 'function' ? footer(footerOriginNode, {
    OkBtn: _ConfirmOkBtn.default,
    CancelBtn: _ConfirmCancelBtn.default
  }) : footerOriginNode))) : footer, /*#__PURE__*/React.createElement(_confirm.default, {
    prefixCls: prefixCls
  }));
}
const ConfirmDialog = props => {
  const {
    close,
    zIndex,
    afterClose,
    open,
    keyboard,
    centered,
    getContainer,
    maskStyle,
    direction,
    prefixCls,
    wrapClassName,
    rootPrefixCls,
    bodyStyle,
    closable = false,
    closeIcon,
    modalRender,
    focusTriggerAfterClose,
    onConfirm,
    styles
  } = props;
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Modal');
    [['visible', 'open'], ['bodyStyle', 'styles.body'], ['maskStyle', 'styles.mask']].forEach(_ref => {
      let [deprecatedName, newName] = _ref;
      warning.deprecated(!(deprecatedName in props), deprecatedName, newName);
    });
  }
  const confirmPrefixCls = `${prefixCls}-confirm`;
  const width = props.width || 416;
  const style = props.style || {};
  const mask = props.mask === undefined ? true : props.mask;
  // 默认为 false，保持旧版默认行为
  const maskClosable = props.maskClosable === undefined ? false : props.maskClosable;
  const classString = (0, _classnames.default)(confirmPrefixCls, `${confirmPrefixCls}-${props.type}`, {
    [`${confirmPrefixCls}-rtl`]: direction === 'rtl'
  }, props.className);
  // ========================= zIndex =========================
  const [, token] = (0, _useToken.default)();
  const mergedZIndex = React.useMemo(() => {
    if (zIndex !== undefined) {
      return zIndex;
    }
    // Static always use max zIndex
    return token.zIndexPopupBase + _useZIndex.CONTAINER_MAX_OFFSET;
  }, [zIndex, token]);
  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(_Modal.default, {
    prefixCls: prefixCls,
    className: classString,
    wrapClassName: (0, _classnames.default)({
      [`${confirmPrefixCls}-centered`]: !!props.centered
    }, wrapClassName),
    onCancel: () => {
      close === null || close === void 0 ? void 0 : close({
        triggerCancel: true
      });
      onConfirm === null || onConfirm === void 0 ? void 0 : onConfirm(false);
    },
    open: open,
    title: "",
    footer: null,
    transitionName: (0, _motion.getTransitionName)(rootPrefixCls || '', 'zoom', props.transitionName),
    maskTransitionName: (0, _motion.getTransitionName)(rootPrefixCls || '', 'fade', props.maskTransitionName),
    mask: mask,
    maskClosable: maskClosable,
    style: style,
    styles: Object.assign({
      body: bodyStyle,
      mask: maskStyle
    }, styles),
    width: width,
    zIndex: mergedZIndex,
    afterClose: afterClose,
    keyboard: keyboard,
    centered: centered,
    getContainer: getContainer,
    closable: closable,
    closeIcon: closeIcon,
    modalRender: modalRender,
    focusTriggerAfterClose: focusTriggerAfterClose
  }, /*#__PURE__*/React.createElement(ConfirmContent, Object.assign({}, props, {
    confirmPrefixCls: confirmPrefixCls
  })));
};
const ConfirmDialogWrapper = props => {
  const {
    rootPrefixCls,
    iconPrefixCls,
    direction,
    theme
  } = props;
  return /*#__PURE__*/React.createElement(_configProvider.default, {
    prefixCls: rootPrefixCls,
    iconPrefixCls: iconPrefixCls,
    direction: direction,
    theme: theme
  }, /*#__PURE__*/React.createElement(ConfirmDialog, Object.assign({}, props)));
};
if (process.env.NODE_ENV !== 'production') {
  ConfirmDialog.displayName = 'ConfirmDialog';
  ConfirmDialogWrapper.displayName = 'ConfirmDialogWrapper';
}
var _default = exports.default = ConfirmDialogWrapper;