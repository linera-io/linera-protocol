"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Footer = void 0;
exports.renderCloseIcon = renderCloseIcon;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _react = _interopRequireDefault(require("react"));
var _CloseOutlined = _interopRequireDefault(require("@ant-design/icons/CloseOutlined"));
var _DisabledContext = require("../config-provider/DisabledContext");
var _locale = require("../locale");
var _NormalCancelBtn = _interopRequireDefault(require("./components/NormalCancelBtn"));
var _NormalOkBtn = _interopRequireDefault(require("./components/NormalOkBtn"));
var _context = require("./context");
var _locale2 = require("./locale");
function renderCloseIcon(prefixCls, closeIcon) {
  return /*#__PURE__*/_react.default.createElement("span", {
    className: `${prefixCls}-close-x`
  }, closeIcon || /*#__PURE__*/_react.default.createElement(_CloseOutlined.default, {
    className: `${prefixCls}-close-icon`
  }));
}
const Footer = props => {
  const {
    okText,
    okType = 'primary',
    cancelText,
    confirmLoading,
    onOk,
    onCancel,
    okButtonProps,
    cancelButtonProps,
    footer
  } = props;
  const [locale] = (0, _locale.useLocale)('Modal', (0, _locale2.getConfirmLocale)());
  // ================== Locale Text ==================
  const okTextLocale = okText || (locale === null || locale === void 0 ? void 0 : locale.okText);
  const cancelTextLocale = cancelText || (locale === null || locale === void 0 ? void 0 : locale.cancelText);
  // ================= Context Value =================
  const btnCtxValue = {
    confirmLoading,
    okButtonProps,
    cancelButtonProps,
    okTextLocale,
    cancelTextLocale,
    okType,
    onOk,
    onCancel
  };
  const btnCtxValueMemo = _react.default.useMemo(() => btnCtxValue, (0, _toConsumableArray2.default)(Object.values(btnCtxValue)));
  let footerNode;
  if (typeof footer === 'function' || typeof footer === 'undefined') {
    footerNode = /*#__PURE__*/_react.default.createElement(_react.default.Fragment, null, /*#__PURE__*/_react.default.createElement(_NormalCancelBtn.default, null), /*#__PURE__*/_react.default.createElement(_NormalOkBtn.default, null));
    if (typeof footer === 'function') {
      footerNode = footer(footerNode, {
        OkBtn: _NormalOkBtn.default,
        CancelBtn: _NormalCancelBtn.default
      });
    }
    footerNode = /*#__PURE__*/_react.default.createElement(_context.ModalContextProvider, {
      value: btnCtxValueMemo
    }, footerNode);
  } else {
    footerNode = footer;
  }
  return /*#__PURE__*/_react.default.createElement(_DisabledContext.DisabledContextProvider, {
    disabled: false
  }, footerNode);
};
exports.Footer = Footer;