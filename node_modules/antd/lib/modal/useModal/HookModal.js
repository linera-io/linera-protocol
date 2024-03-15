"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../../config-provider");
var _en_US = _interopRequireDefault(require("../../locale/en_US"));
var _useLocale = _interopRequireDefault(require("../../locale/useLocale"));
var _ConfirmDialog = _interopRequireDefault(require("../ConfirmDialog"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const HookModal = (_a, ref) => {
  var _b;
  var {
      afterClose: hookAfterClose,
      config
    } = _a,
    restProps = __rest(_a, ["afterClose", "config"]);
  const [open, setOpen] = React.useState(true);
  const [innerConfig, setInnerConfig] = React.useState(config);
  const {
    direction,
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('modal');
  const rootPrefixCls = getPrefixCls();
  const afterClose = () => {
    var _a;
    hookAfterClose();
    (_a = innerConfig.afterClose) === null || _a === void 0 ? void 0 : _a.call(innerConfig);
  };
  const close = function () {
    setOpen(false);
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }
    const triggerCancel = args.some(param => param && param.triggerCancel);
    if (innerConfig.onCancel && triggerCancel) {
      innerConfig.onCancel.apply(innerConfig, [() => {}].concat((0, _toConsumableArray2.default)(args.slice(1))));
    }
  };
  React.useImperativeHandle(ref, () => ({
    destroy: close,
    update: newConfig => {
      setInnerConfig(originConfig => Object.assign(Object.assign({}, originConfig), newConfig));
    }
  }));
  const mergedOkCancel = (_b = innerConfig.okCancel) !== null && _b !== void 0 ? _b : innerConfig.type === 'confirm';
  const [contextLocale] = (0, _useLocale.default)('Modal', _en_US.default.Modal);
  return /*#__PURE__*/React.createElement(_ConfirmDialog.default, Object.assign({
    prefixCls: prefixCls,
    rootPrefixCls: rootPrefixCls
  }, innerConfig, {
    close: close,
    open: open,
    afterClose: afterClose,
    okText: innerConfig.okText || (mergedOkCancel ? contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.okText : contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.justOkText),
    direction: innerConfig.direction || direction,
    cancelText: innerConfig.cancelText || (contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.cancelText)
  }, restProps));
};
var _default = exports.default = /*#__PURE__*/React.forwardRef(HookModal);