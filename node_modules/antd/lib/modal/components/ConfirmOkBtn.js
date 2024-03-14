"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _ActionButton = _interopRequireDefault(require("../../_util/ActionButton"));
var _context = require("../context");
const ConfirmOkBtn = () => {
  const {
    autoFocusButton,
    close,
    isSilent,
    okButtonProps,
    rootPrefixCls,
    okTextLocale,
    okType,
    onConfirm,
    onOk
  } = (0, _react.useContext)(_context.ModalContext);
  return /*#__PURE__*/_react.default.createElement(_ActionButton.default, {
    isSilent: isSilent,
    type: okType || 'primary',
    actionFn: onOk,
    close: function () {
      close === null || close === void 0 ? void 0 : close.apply(void 0, arguments);
      onConfirm === null || onConfirm === void 0 ? void 0 : onConfirm(true);
    },
    autoFocus: autoFocusButton === 'ok',
    buttonProps: okButtonProps,
    prefixCls: `${rootPrefixCls}-btn`
  }, okTextLocale);
};
var _default = exports.default = ConfirmOkBtn;