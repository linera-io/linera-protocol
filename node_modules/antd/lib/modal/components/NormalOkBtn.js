"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _button = _interopRequireDefault(require("../../button"));
var _buttonHelpers = require("../../button/buttonHelpers");
var _context = require("../context");
const NormalOkBtn = () => {
  const {
    confirmLoading,
    okButtonProps,
    okType,
    okTextLocale,
    onOk
  } = (0, _react.useContext)(_context.ModalContext);
  return /*#__PURE__*/_react.default.createElement(_button.default, Object.assign({}, (0, _buttonHelpers.convertLegacyProps)(okType), {
    loading: confirmLoading,
    onClick: onOk
  }, okButtonProps), okTextLocale);
};
var _default = exports.default = NormalOkBtn;